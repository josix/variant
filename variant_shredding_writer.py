import decimal
from typing import List, Optional, Any
from abc import ABC, abstractmethod

from variant import Variant
from variant_util import VariantUtil, Type, malformed_variant
from variant_schema import VariantSchema
from variant_builder import VariantBuilder

class ShreddedResult(ABC):
    """
    Interface to build up a shredded result. Callers should implement a ShreddedResultBuilder to
    create an empty result with a given schema. The castShredded method will call one or more of
    the add* methods to populate it.
    """
    
    @abstractmethod
    def add_array(self, array: List['ShreddedResult']) -> None:
        """Create an array. The elements are the result of shredding each element."""
        pass
    
    @abstractmethod
    def add_object(self, values: List['ShreddedResult']) -> None:
        """
        Create an object. The values are the result of shredding each field, order by the index in
        objectSchema. Missing fields are populated with an empty result.
        """
        pass
    
    @abstractmethod
    def add_variant_value(self, result: bytes) -> None:
        """Add a variant value."""
        pass
    
    @abstractmethod
    def add_scalar(self, result: Any) -> None:
        """
        Add a scalar to typed_value. The type of Object depends on the scalarSchema in the shredding
        schema.
        """
        pass
    
    @abstractmethod
    def add_metadata(self, result: bytes) -> None:
        """Add metadata."""
        pass

class ShreddedResultBuilder(ABC):
    """Interface for building shredded results."""
    
    @abstractmethod
    def create_empty(self, schema: VariantSchema) -> ShreddedResult:
        """Create an empty shredded result with the given schema."""
        pass
    
    @abstractmethod
    def allow_numeric_scale_changes(self) -> bool:
        """
        If true, we will shred decimals to a different scale or to integers, as long as they are
        numerically equivalent. Similarly, integers will be allowed to shred to decimals.
        """
        pass

class VariantShreddingWriter:
    """Class to implement shredding a Variant value."""
    
    @staticmethod
    def cast_shredded(v: Variant, schema: VariantSchema, builder: ShreddedResultBuilder) -> ShreddedResult:
        """
        Converts an input variant into shredded components. Returns the shredded result, as well
        as the original Variant with shredded fields removed.
        `schema` must be a valid shredding schema, as described in
        https://github.com/apache/parquet-format/blob/master/VariantShredding.md.
        """
        variant_type = v.get_type()
        result = builder.create_empty(schema)
        
        if schema.top_level_metadata_idx >= 0:
            result.add_metadata(v.get_metadata())
        
        if schema.array_schema is not None and variant_type == Type.ARRAY:
            # The array element is always a struct containing untyped and typed fields
            element_schema = schema.array_schema
            size = v.array_size()
            array = []
            
            for i in range(size):
                shredded_array = VariantShreddingWriter.cast_shredded(
                    v.get_element_at_index(i), element_schema, builder
                )
                array.append(shredded_array)
            
            result.add_array(array)
        
        elif schema.object_schema is not None and variant_type == Type.OBJECT:
            object_schema = schema.object_schema
            shredded_values = [None] * len(object_schema)
            
            # Create a variant builder for any field that exists in `v`, but not in the shredding schema
            variant_builder = VariantBuilder(False)
            field_entries = []
            
            # Keep track of which schema fields we actually found in the Variant value
            num_fields_matched = 0
            start = variant_builder.get_write_pos()
            
            for i in range(v.object_size()):
                field = v.get_field_at_index(i)
                field_idx = schema.object_schema_map.get(field.key)
                
                if field_idx is not None:
                    # The field exists in the shredding schema. Recursively shred, and write the result
                    shredded_field = VariantShreddingWriter.cast_shredded(
                        field.value, object_schema[field_idx].schema, builder
                    )
                    shredded_values[field_idx] = shredded_field
                    num_fields_matched += 1
                else:
                    # The field is not shredded. Put it in the untyped_value column
                    id_val = v.get_dictionary_id_at_index(i)
                    field_entries.append(
                        VariantBuilder.FieldEntry(
                            field.key, id_val, variant_builder.get_write_pos() - start
                        )
                    )
                    # shallowAppendVariant is needed for correctness, since we're relying on the metadata IDs
                    # being unchanged
                    variant_builder.shallow_append_variant(field.value)
            
            if num_fields_matched < len(object_schema):
                # Set missing fields to non-null with all fields set to null
                for i in range(len(object_schema)):
                    if shredded_values[i] is None:
                        field_schema = object_schema[i]
                        empty_child = builder.create_empty(field_schema.schema)
                        shredded_values[i] = empty_child
                        num_fields_matched += 1
            
            if num_fields_matched != len(object_schema):
                # Since we just filled in all the null entries, this can only happen if we tried to write
                # to the same field twice; i.e. the Variant contained duplicate fields, which is invalid
                raise malformed_variant()
            
            result.add_object(shredded_values)
            
            if variant_builder.get_write_pos() != start:
                # We added something to the untyped value
                variant_builder.finish_writing_object(start, field_entries)
                result.add_variant_value(variant_builder.value_without_metadata())
        
        elif schema.scalar_schema is not None:
            scalar_type = schema.scalar_schema
            typed_value = VariantShreddingWriter._try_typed_shred(v, variant_type, scalar_type, builder)
            
            if typed_value is not None:
                # Store the typed value
                result.add_scalar(typed_value)
            else:
                result.add_variant_value(v.get_value())
        
        else:
            # Store in untyped
            result.add_variant_value(v.get_value())
        
        return result
    
    @staticmethod
    def _try_typed_shred(
        v: Variant, 
        variant_type: Type, 
        target_type: VariantSchema.ScalarType,
        builder: ShreddedResultBuilder
    ) -> Optional[Any]:
        """
        Tries to cast a Variant into a typed value. If the cast fails, returns None.
        
        Args:
            v: The variant to cast
            variant_type: The Variant Type of v
            target_type: The target type
            builder: The shredded result builder
            
        Returns:
            The scalar value, or None if the cast is not valid.
        """
        if variant_type == Type.LONG:
            if isinstance(target_type, VariantSchema.IntegralType):
                # Check that the target type can hold the actual value
                size = target_type.size
                value = v.get_long()
                
                if size == VariantSchema.IntegralSize.BYTE:
                    if -128 <= value <= 127:
                        return value
                elif size == VariantSchema.IntegralSize.SHORT:
                    if -32768 <= value <= 32767:
                        return value
                elif size == VariantSchema.IntegralSize.INT:
                    if -2147483648 <= value <= 2147483647:
                        return value
                elif size == VariantSchema.IntegralSize.LONG:
                    return value
            
            elif (isinstance(target_type, VariantSchema.DecimalType) and 
                  builder.allow_numeric_scale_changes()):
                # If the integer can fit in the given decimal precision, allow it
                value = v.get_long()
                # Set to the requested scale, and check if the precision is large enough
                decimal_value = decimal.Decimal(value)
                scaled_value = decimal_value.scaleb(target_type.scale)
                # The initial value should have scale 0, so rescaling shouldn't lose information
                assert decimal_value == scaled_value
                if len(str(scaled_value.normalize())) - 1 <= target_type.precision:
                    return scaled_value
        
        elif variant_type == Type.DECIMAL:
            if isinstance(target_type, VariantSchema.DecimalType):
                # Use get_decimal_with_original_scale so that we retain scale information if
                # allow_numeric_scale_changes() is false
                value = VariantUtil.get_decimal_with_original_scale(v.value, v.pos)
                
                if (len(str(value.normalize())) - 1 <= target_type.precision and 
                    value.as_tuple().exponent == -target_type.scale):
                    return value
                
                if builder.allow_numeric_scale_changes():
                    # Convert to the target scale, and see if it fits
                    try:
                        scaled_value = value.scaleb(value.as_tuple().exponent + target_type.scale)
                        if (scaled_value == value and 
                            len(str(scaled_value.normalize())) - 1 <= target_type.precision):
                            return scaled_value
                    except:
                        pass
            
            elif (isinstance(target_type, VariantSchema.IntegralType) and 
                  builder.allow_numeric_scale_changes()):
                # Check if the decimal happens to be an integer
                value = v.get_decimal()
                size = target_type.size
                
                # Try to cast to the appropriate type, and check if any information is lost
                if size == VariantSchema.IntegralSize.BYTE:
                    try:
                        byte_val = int(value)
                        if -128 <= byte_val <= 127 and decimal.Decimal(byte_val) == value:
                            return byte_val
                    except:
                        pass
                elif size == VariantSchema.IntegralSize.SHORT:
                    try:
                        short_val = int(value)
                        if -32768 <= short_val <= 32767 and decimal.Decimal(short_val) == value:
                            return short_val
                    except:
                        pass
                elif size == VariantSchema.IntegralSize.INT:
                    try:
                        int_val = int(value)
                        if -2147483648 <= int_val <= 2147483647 and decimal.Decimal(int_val) == value:
                            return int_val
                    except:
                        pass
                elif size == VariantSchema.IntegralSize.LONG:
                    try:
                        long_val = int(value)
                        if decimal.Decimal(long_val) == value:
                            return long_val
                    except:
                        pass
        
        elif variant_type == Type.BOOLEAN:
            if isinstance(target_type, VariantSchema.BooleanType):
                return v.get_boolean()
        
        elif variant_type == Type.STRING:
            if isinstance(target_type, VariantSchema.StringType):
                return v.get_string()
        
        elif variant_type == Type.DOUBLE:
            if isinstance(target_type, VariantSchema.DoubleType):
                return v.get_double()
        
        elif variant_type == Type.DATE:
            if isinstance(target_type, VariantSchema.DateType):
                return v.get_long()
        
        elif variant_type == Type.TIMESTAMP:
            if isinstance(target_type, VariantSchema.TimestampType):
                return v.get_long()
        
        elif variant_type == Type.TIMESTAMP_NTZ:
            if isinstance(target_type, VariantSchema.TimestampNTZType):
                return v.get_long()
        
        elif variant_type == Type.FLOAT:
            if isinstance(target_type, VariantSchema.FloatType):
                return v.get_float()
        
        elif variant_type == Type.BINARY:
            if isinstance(target_type, VariantSchema.BinaryType):
                return v.get_binary()
        
        elif variant_type == Type.UUID:
            if isinstance(target_type, VariantSchema.UuidType):
                return v.get_uuid()
        
        # The stored type does not match the requested shredding type
        # Return None, and the caller will store the result in untyped_value
        return None

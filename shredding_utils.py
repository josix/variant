import decimal
import uuid
from abc import ABC, abstractmethod

from variant import Variant
from variant_util import Type, malformed_variant
from variant_schema import VariantSchema
from variant_builder import VariantBuilder

class ShreddedRow(ABC):
    """
    Interface to read from a shredded result. It essentially has the same interface and semantics
    as Spark's `SpecializedGetters`, but we need a new interface to avoid the dependency.
    """
    
    @abstractmethod
    def is_null_at(self, ordinal: int) -> bool:
        """Check if the value at the given ordinal is null."""
        pass
    
    @abstractmethod
    def get_boolean(self, ordinal: int) -> bool:
        """Get a boolean value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_byte(self, ordinal: int) -> int:
        """Get a byte value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_short(self, ordinal: int) -> int:
        """Get a short value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_int(self, ordinal: int) -> int:
        """Get an int value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_long(self, ordinal: int) -> int:
        """Get a long value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_float(self, ordinal: int) -> float:
        """Get a float value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_double(self, ordinal: int) -> float:
        """Get a double value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_decimal(self, ordinal: int, precision: int, scale: int) -> decimal.Decimal:
        """Get a decimal value at the given ordinal with the specified precision and scale."""
        pass
    
    @abstractmethod
    def get_string(self, ordinal: int) -> str:
        """Get a string value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_binary(self, ordinal: int) -> bytes:
        """Get a binary value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_uuid(self, ordinal: int) -> uuid.UUID:
        """Get a UUID value at the given ordinal."""
        pass
    
    @abstractmethod
    def get_struct(self, ordinal: int, num_fields: int) -> 'ShreddedRow':
        """Get a struct value at the given ordinal with the specified number of fields."""
        pass
    
    @abstractmethod
    def get_array(self, ordinal: int) -> 'ShreddedRow':
        """Get an array value at the given ordinal."""
        pass
    
    @abstractmethod
    def num_elements(self) -> int:
        """Get the number of elements in this row if it's an array."""
        pass

class ShreddingUtils:
    """Utility functions for shredding and rebuilding variants."""
    
    @staticmethod
    def rebuild(row: ShreddedRow, schema: VariantSchema) -> Variant:
        """
        This `rebuild` function should only be called on the top-level schema, and that other private
        implementation will be called on any recursively shredded sub-schema.
        """
        if schema.top_level_metadata_idx < 0 or row.is_null_at(schema.top_level_metadata_idx):
            raise malformed_variant()
        
        metadata = row.get_binary(schema.top_level_metadata_idx)
        
        if schema.is_unshredded():
            # `rebuild` is unnecessary for unshredded variant
            if row.is_null_at(schema.variant_idx):
                raise malformed_variant()
            return Variant(row.get_binary(schema.variant_idx), metadata)
        
        builder = VariantBuilder(False)
        ShreddingUtils._rebuild(row, metadata, schema, builder)
        return builder.result()
    
    @staticmethod
    def _rebuild(row: ShreddedRow, metadata: bytes, schema: VariantSchema, builder: VariantBuilder) -> None:
        """
        Rebuild a variant value from the shredded data according to the reconstruction algorithm in
        https://github.com/apache/parquet-format/blob/master/VariantShredding.md.
        Append the result to `builder`.
        """
        typed_idx = schema.typed_idx
        variant_idx = schema.variant_idx
        
        if typed_idx >= 0 and not row.is_null_at(typed_idx):
            if schema.scalar_schema is not None:
                scalar = schema.scalar_schema
                
                if isinstance(scalar, VariantSchema.StringType):
                    builder.append_string(row.get_string(typed_idx))
                
                elif isinstance(scalar, VariantSchema.IntegralType):
                    it = scalar
                    value = 0
                    
                    if it.size == VariantSchema.IntegralSize.BYTE:
                        value = row.get_byte(typed_idx)
                    elif it.size == VariantSchema.IntegralSize.SHORT:
                        value = row.get_short(typed_idx)
                    elif it.size == VariantSchema.IntegralSize.INT:
                        value = row.get_int(typed_idx)
                    elif it.size == VariantSchema.IntegralSize.LONG:
                        value = row.get_long(typed_idx)
                    
                    builder.append_long(value)
                
                elif isinstance(scalar, VariantSchema.FloatType):
                    builder.append_float(row.get_float(typed_idx))
                
                elif isinstance(scalar, VariantSchema.DoubleType):
                    builder.append_double(row.get_double(typed_idx))
                
                elif isinstance(scalar, VariantSchema.BooleanType):
                    builder.append_boolean(row.get_boolean(typed_idx))
                
                elif isinstance(scalar, VariantSchema.BinaryType):
                    builder.append_binary(row.get_binary(typed_idx))
                
                elif isinstance(scalar, VariantSchema.UuidType):
                    builder.append_uuid(row.get_uuid(typed_idx))
                
                elif isinstance(scalar, VariantSchema.DecimalType):
                    dt = scalar
                    builder.append_decimal(row.get_decimal(typed_idx, dt.precision, dt.scale))
                
                elif isinstance(scalar, VariantSchema.DateType):
                    builder.append_date(row.get_int(typed_idx))
                
                elif isinstance(scalar, VariantSchema.TimestampType):
                    builder.append_timestamp(row.get_long(typed_idx))
                
                else:
                    assert isinstance(scalar, VariantSchema.TimestampNTZType)
                    builder.append_timestamp_ntz(row.get_long(typed_idx))
            
            elif schema.array_schema is not None:
                element_schema = schema.array_schema
                array = row.get_array(typed_idx)
                start = builder.get_write_pos()
                offsets = []
                
                for i in range(array.num_elements()):
                    offsets.append(builder.get_write_pos() - start)
                    ShreddingUtils._rebuild(
                        array.get_struct(i, element_schema.num_fields), 
                        metadata, 
                        element_schema, 
                        builder
                    )
                
                builder.finish_writing_array(start, offsets)
            
            else:  # Object
                object_row = row.get_struct(typed_idx, len(schema.object_schema))
                fields = []
                start = builder.get_write_pos()
                
                for field_idx in range(len(schema.object_schema)):
                    # Shredded field must not be null
                    if object_row.is_null_at(field_idx):
                        raise malformed_variant()
                    
                    field_name = schema.object_schema[field_idx].field_name
                    field_schema = schema.object_schema[field_idx].schema
                    field_value = object_row.get_struct(field_idx, field_schema.num_fields)
                    
                    # If the field doesn't have non-null `typed_value` or `value`, it is missing
                    if ((field_schema.typed_idx >= 0 and not field_value.is_null_at(field_schema.typed_idx)) or
                        (field_schema.variant_idx >= 0 and not field_value.is_null_at(field_schema.variant_idx))):
                        id_val = builder.add_key(field_name)
                        fields.append(VariantBuilder.FieldEntry(field_name, id_val, builder.get_write_pos() - start))
                        ShreddingUtils._rebuild(field_value, metadata, field_schema, builder)
                
                if variant_idx >= 0 and not row.is_null_at(variant_idx):
                    # Add the leftover fields in the variant binary
                    v = Variant(row.get_binary(variant_idx), metadata)
                    
                    if v.get_type() != Type.OBJECT:
                        raise malformed_variant()
                    
                    for i in range(v.object_size()):
                        field = v.get_field_at_index(i)
                        
                        # `value` must not contain any shredded field
                        if field.key in schema.object_schema_map:
                            raise malformed_variant()
                        
                        id_val = builder.add_key(field.key)
                        fields.append(VariantBuilder.FieldEntry(field.key, id_val, builder.get_write_pos() - start))
                        builder.append_variant(field.value)
                
                builder.finish_writing_object(start, fields)
        
        elif variant_idx >= 0 and not row.is_null_at(variant_idx):
            # `typed_value` doesn't exist or is null. Read from `value`
            builder.append_variant(Variant(row.get_binary(variant_idx), metadata))
        
        else:
            # This means the variant is missing in a context where it must present, so the input data is invalid
            raise malformed_variant()

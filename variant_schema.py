from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

class VariantSchema:
    """
    Defines a valid shredding schema, as described in
    https://github.com/apache/parquet-format/blob/master/VariantShredding.md.
    A shredding schema contains a value and optional typed_value field.
    If a typed_value is an array or struct, it recursively contain its own shredding schema for
    elements and fields, respectively.
    The schema also contains a metadata field at the top level, but not in recursively shredded
    fields.
    """
    
    @dataclass
    class ObjectField:
        """Represents one field of an object in the shredding schema."""
        field_name: str
        schema: 'VariantSchema'
        
        def __str__(self) -> str:
            return f"ObjectField{{fieldName={self.field_name}, schema={self.schema}}}"
    
    class ScalarType:
        """Base class for scalar types in the schema"""
        pass
    
    class StringType(ScalarType):
        """String type in the schema"""
        pass
    
    class IntegralSize(Enum):
        """Enum for integral sizes"""
        BYTE = 1
        SHORT = 2
        INT = 3
        LONG = 4
    
    @dataclass
    class IntegralType(ScalarType):
        """Integral type in the schema"""
        size: 'VariantSchema.IntegralSize'
    
    class FloatType(ScalarType):
        """Float type in the schema"""
        pass
    
    class DoubleType(ScalarType):
        """Double type in the schema"""
        pass
    
    class BooleanType(ScalarType):
        """Boolean type in the schema"""
        pass
    
    class BinaryType(ScalarType):
        """Binary type in the schema"""
        pass
    
    @dataclass
    class DecimalType(ScalarType):
        """Decimal type in the schema"""
        precision: int
        scale: int
    
    class DateType(ScalarType):
        """Date type in the schema"""
        pass
    
    class TimestampType(ScalarType):
        """Timestamp type in the schema"""
        pass
    
    class TimestampNTZType(ScalarType):
        """Timestamp without timezone type in the schema"""
        pass
    
    class UuidType(ScalarType):
        """UUID type in the schema"""
        pass
    
    def __init__(self, 
                 typed_idx: int, 
                 variant_idx: int, 
                 top_level_metadata_idx: int, 
                 num_fields: int,
                 scalar_schema: Optional[ScalarType] = None, 
                 object_schema: Optional[List[ObjectField]] = None,
                 array_schema: Optional['VariantSchema'] = None):
        """
        Initialize a VariantSchema.
        
        Args:
            typed_idx: The index of the typed_value field in the schema. -1 if not present.
            variant_idx: The index of the value field in the schema. -1 if not present.
            top_level_metadata_idx: The index of the metadata field. Must be non-negative at top level, -1 elsewhere.
            num_fields: The number of fields in the schema (1-3).
            scalar_schema: The scalar schema if this is a scalar type.
            object_schema: The object schema if this is an object type.
            array_schema: The array schema if this is an array type.
        """
        self.typed_idx = typed_idx
        self.variant_idx = variant_idx
        self.top_level_metadata_idx = top_level_metadata_idx
        self.num_fields = num_fields
        self.scalar_schema = scalar_schema
        self.object_schema = object_schema
        
        # Build a map for fast lookup of object fields by name
        if object_schema:
            self.object_schema_map = {field.field_name: i for i, field in enumerate(object_schema)}
        else:
            self.object_schema_map = {}
        
        self.array_schema = array_schema
    
    def is_unshredded(self) -> bool:
        """
        Return whether the variant column is unshredded.
        The user is not required to do anything special, but can have certain optimizations for unshredded variant.
        """
        return self.top_level_metadata_idx >= 0 and self.variant_idx >= 0 and self.typed_idx < 0
    
    def __str__(self) -> str:
        return (f"VariantSchema{{typedIdx={self.typed_idx}, variantIdx={self.variant_idx}, "
                f"topLevelMetadataIdx={self.top_level_metadata_idx}, numFields={self.num_fields}, "
                f"scalarSchema={self.scalar_schema}, objectSchema={self.object_schema}, "
                f"arraySchema={self.array_schema}}}")

import enum
import struct
import decimal
import uuid
from typing import Callable, TypeVar

# Constants for variant format
BASIC_TYPE_BITS = 2
BASIC_TYPE_MASK = 0x3
TYPE_INFO_MASK = 0x3F
MAX_SHORT_STR_SIZE = 0x3F

# Basic type values
PRIMITIVE = 0
SHORT_STR = 1
OBJECT = 2
ARRAY = 3

# Type info values for PRIMITIVE
NULL = 0
TRUE = 1
FALSE = 2
INT1 = 3
INT2 = 4
INT4 = 5
INT8 = 6
DOUBLE = 7
DECIMAL4 = 8
DECIMAL8 = 9
DECIMAL16 = 10
DATE = 11
TIMESTAMP = 12
TIMESTAMP_NTZ = 13
FLOAT = 14
BINARY = 15
LONG_STR = 16
UUID = 20

# Version info
VERSION = 1
VERSION_MASK = 0x0F

# Size limits
U8_MAX = 0xFF
U16_MAX = 0xFFFF
U24_MAX = 0xFFFFFF
U24_SIZE = 3
U32_SIZE = 4

# Size limit for variant value and metadata (16MiB)
SIZE_LIMIT = U24_MAX + 1

# Decimal precision limits
MAX_DECIMAL4_PRECISION = 9
MAX_DECIMAL8_PRECISION = 18
MAX_DECIMAL16_PRECISION = 38

class Type(enum.Enum):
    """Enum for variant value types"""
    OBJECT = 1
    ARRAY = 2
    NULL = 3
    BOOLEAN = 4
    LONG = 5
    STRING = 6
    DOUBLE = 7
    DECIMAL = 8
    DATE = 9
    TIMESTAMP = 10
    TIMESTAMP_NTZ = 11
    FLOAT = 12
    BINARY = 13
    UUID = 14

class VariantException(Exception):
    """Base exception for variant-related errors"""
    pass

class MalformedVariantException(VariantException):
    """Exception for malformed variant data"""
    def __init__(self):
        super().__init__("MALFORMED_VARIANT")

class VariantConstructorSizeLimitException(VariantException):
    """Exception for variant size limit exceeded"""
    def __init__(self):
        super().__init__("VARIANT_CONSTRUCTOR_SIZE_LIMIT")

class UnknownPrimitiveTypeException(VariantException):
    """Exception for unknown primitive type"""
    def __init__(self, type_id):
        super().__init__(f"UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT: {type_id}")

def malformed_variant():
    """Create a malformed variant exception"""
    return MalformedVariantException()

def variant_constructor_size_limit():
    """Create a size limit exception"""
    return VariantConstructorSizeLimitException()

def unknown_primitive_type_in_variant(type_id):
    """Create an unknown primitive type exception"""
    return UnknownPrimitiveTypeException(type_id)

T = TypeVar('T')

class VariantUtil:
    """Utility functions for variant manipulation"""
    
    @staticmethod
    def write_long(bytes_array: bytearray, pos: int, value: int, num_bytes: int) -> None:
        """Write the least significant `num_bytes` bytes in `value` into `bytes_array[pos:pos+num_bytes]` in little endian."""
        for i in range(num_bytes):
            bytes_array[pos + i] = (value >> (8 * i)) & 0xFF

    @staticmethod
    def primitive_header(type_val: int) -> int:
        """Create a primitive header byte"""
        return (type_val << 2) | PRIMITIVE

    @staticmethod
    def short_str_header(size: int) -> int:
        """Create a short string header byte"""
        return (size << 2) | SHORT_STR

    @staticmethod
    def object_header(large_size: bool, id_size: int, offset_size: int) -> int:
        """Create an object header byte"""
        return (((1 if large_size else 0) << (BASIC_TYPE_BITS + 4)) |
                ((id_size - 1) << (BASIC_TYPE_BITS + 2)) |
                ((offset_size - 1) << BASIC_TYPE_BITS) | OBJECT)

    @staticmethod
    def array_header(large_size: bool, offset_size: int) -> int:
        """Create an array header byte"""
        return (((1 if large_size else 0) << (BASIC_TYPE_BITS + 2)) |
                ((offset_size - 1) << BASIC_TYPE_BITS) | ARRAY)

    @staticmethod
    def check_index(pos: int, length: int) -> None:
        """Check if an index is valid"""
        if pos < 0 or pos >= length:
            raise malformed_variant()

    @staticmethod
    def read_long(bytes_array: bytes, pos: int, num_bytes: int) -> int:
        """Read a little-endian signed long value"""
        VariantUtil.check_index(pos, len(bytes_array))
        VariantUtil.check_index(pos + num_bytes - 1, len(bytes_array))
        
        result = 0
        # All bytes except the most significant byte should be unsign-extended
        for i in range(num_bytes - 1):
            unsigned_byte_value = bytes_array[pos + i] & 0xFF
            result |= unsigned_byte_value << (8 * i)
        
        # The most significant byte should be sign-extended
        signed_byte_value = bytes_array[pos + num_bytes - 1]
        result |= signed_byte_value << (8 * (num_bytes - 1))
        
        return result

    @staticmethod
    def read_unsigned(bytes_array: bytes, pos: int, num_bytes: int) -> int:
        """Read a little-endian unsigned int value"""
        VariantUtil.check_index(pos, len(bytes_array))
        VariantUtil.check_index(pos + num_bytes - 1, len(bytes_array))
        
        result = 0
        for i in range(num_bytes):
            unsigned_byte_value = bytes_array[pos + i] & 0xFF
            result |= unsigned_byte_value << (8 * i)
        
        if result < 0:
            raise malformed_variant()
        
        return result

    @staticmethod
    def get_type_info(value: bytes, pos: int) -> int:
        """Get the type info bits from a variant value"""
        VariantUtil.check_index(pos, len(value))
        return (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK

    @staticmethod
    def get_type(value: bytes, pos: int) -> Type:
        """Get the value type of variant value"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type == SHORT_STR:
            return Type.STRING
        elif basic_type == OBJECT:
            return Type.OBJECT
        elif basic_type == ARRAY:
            return Type.ARRAY
        else:  # PRIMITIVE
            if type_info == NULL:
                return Type.NULL
            elif type_info in (TRUE, FALSE):
                return Type.BOOLEAN
            elif type_info in (INT1, INT2, INT4, INT8):
                return Type.LONG
            elif type_info == DOUBLE:
                return Type.DOUBLE
            elif type_info in (DECIMAL4, DECIMAL8, DECIMAL16):
                return Type.DECIMAL
            elif type_info == DATE:
                return Type.DATE
            elif type_info == TIMESTAMP:
                return Type.TIMESTAMP
            elif type_info == TIMESTAMP_NTZ:
                return Type.TIMESTAMP_NTZ
            elif type_info == FLOAT:
                return Type.FLOAT
            elif type_info == BINARY:
                return Type.BINARY
            elif type_info == LONG_STR:
                return Type.STRING
            elif type_info == UUID:
                return Type.UUID
            else:
                raise unknown_primitive_type_in_variant(type_info)

    @staticmethod
    def value_size(value: bytes, pos: int) -> int:
        """Compute the size in bytes of the variant value"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type == SHORT_STR:
            return 1 + type_info
        elif basic_type == OBJECT:
            return VariantUtil.handle_object(
                value, pos,
                lambda size, id_size, offset_size, id_start, offset_start, data_start:
                    data_start - pos + VariantUtil.read_unsigned(
                        value, offset_start + size * offset_size, offset_size
                    )
            )
        elif basic_type == ARRAY:
            return VariantUtil.handle_array(
                value, pos,
                lambda size, offset_size, offset_start, data_start:
                    data_start - pos + VariantUtil.read_unsigned(
                        value, offset_start + size * offset_size, offset_size
                    )
            )
        else:  # PRIMITIVE
            if type_info in (NULL, TRUE, FALSE):
                return 1
            elif type_info == INT1:
                return 2
            elif type_info == INT2:
                return 3
            elif type_info in (INT4, DATE, FLOAT):
                return 5
            elif type_info in (INT8, DOUBLE, TIMESTAMP, TIMESTAMP_NTZ):
                return 9
            elif type_info == DECIMAL4:
                return 6
            elif type_info == DECIMAL8:
                return 10
            elif type_info == DECIMAL16:
                return 18
            elif type_info in (BINARY, LONG_STR):
                return 1 + U32_SIZE + VariantUtil.read_unsigned(value, pos + 1, U32_SIZE)
            elif type_info == UUID:
                return 17
            else:
                raise unknown_primitive_type_in_variant(type_info)

    @staticmethod
    def unexpected_type(expected_type: Type) -> Exception:
        """Create an exception for unexpected type"""
        return ValueError(f"Expected type to be {expected_type}")

    @staticmethod
    def get_boolean(value: bytes, pos: int) -> bool:
        """Get a boolean value from variant value"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type != PRIMITIVE or (type_info != TRUE and type_info != FALSE):
            raise VariantUtil.unexpected_type(Type.BOOLEAN)
        
        return type_info == TRUE

    @staticmethod
    def get_long(value: bytes, pos: int) -> int:
        """Get a long value from variant value"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type != PRIMITIVE:
            raise ValueError("Expected type to be LONG/DATE/TIMESTAMP/TIMESTAMP_NTZ")
        
        if type_info == INT1:
            return VariantUtil.read_long(value, pos + 1, 1)
        elif type_info == INT2:
            return VariantUtil.read_long(value, pos + 1, 2)
        elif type_info in (INT4, DATE):
            return VariantUtil.read_long(value, pos + 1, 4)
        elif type_info in (INT8, TIMESTAMP, TIMESTAMP_NTZ):
            return VariantUtil.read_long(value, pos + 1, 8)
        else:
            raise ValueError("Expected type to be LONG/DATE/TIMESTAMP/TIMESTAMP_NTZ")

    @staticmethod
    def get_double(value: bytes, pos: int) -> float:
        """Get a double value from variant value"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type != PRIMITIVE or type_info != DOUBLE:
            raise VariantUtil.unexpected_type(Type.DOUBLE)
        
        # Read 8 bytes as a double
        return struct.unpack('<d', value[pos+1:pos+9])[0]

    @staticmethod
    def check_decimal(d: decimal.Decimal, max_precision: int) -> None:
        """Check whether the precision and scale of the decimal are within the limit"""
        if d.as_tuple().exponent < -max_precision or len(d.as_tuple().digits) > max_precision:
            raise malformed_variant()

    @staticmethod
    def get_decimal_with_original_scale(value: bytes, pos: int) -> decimal.Decimal:
        """Get a decimal value from variant value with original scale"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type != PRIMITIVE:
            raise VariantUtil.unexpected_type(Type.DECIMAL)
        
        # Interpret the scale byte as unsigned
        scale = value[pos + 1] & 0xFF
        
        if type_info == DECIMAL4:
            result = decimal.Decimal(VariantUtil.read_long(value, pos + 2, 4)) / decimal.Decimal(10) ** scale
            VariantUtil.check_decimal(result, MAX_DECIMAL4_PRECISION)
        elif type_info == DECIMAL8:
            result = decimal.Decimal(VariantUtil.read_long(value, pos + 2, 8)) / decimal.Decimal(10) ** scale
            VariantUtil.check_decimal(result, MAX_DECIMAL8_PRECISION)
        elif type_info == DECIMAL16:
            VariantUtil.check_index(pos + 17, len(value))
            # Copy bytes in reverse order for big-endian representation
            bytes_val = bytearray(16)
            for i in range(16):
                bytes_val[i] = value[pos + 17 - i]
            
            # Convert to integer and then to decimal with scale
            int_val = int.from_bytes(bytes_val, byteorder='big', signed=True)
            result = decimal.Decimal(int_val) / decimal.Decimal(10) ** scale
            VariantUtil.check_decimal(result, MAX_DECIMAL16_PRECISION)
        else:
            raise VariantUtil.unexpected_type(Type.DECIMAL)
        
        return result

    @staticmethod
    def get_decimal(value: bytes, pos: int) -> decimal.Decimal:
        """Get a decimal value from variant value with trailing zeros stripped"""
        return VariantUtil.get_decimal_with_original_scale(value, pos).normalize()

    @staticmethod
    def get_float(value: bytes, pos: int) -> float:
        """Get a float value from variant value"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type != PRIMITIVE or type_info != FLOAT:
            raise VariantUtil.unexpected_type(Type.FLOAT)
        
        # Read 4 bytes as a float
        return struct.unpack('<f', value[pos+1:pos+5])[0]

    @staticmethod
    def get_binary(value: bytes, pos: int) -> bytes:
        """Get a binary value from variant value"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type != PRIMITIVE or type_info != BINARY:
            raise VariantUtil.unexpected_type(Type.BINARY)
        
        start = pos + 1 + U32_SIZE
        length = VariantUtil.read_unsigned(value, pos + 1, U32_SIZE)
        VariantUtil.check_index(start + length - 1, len(value))
        
        return value[start:start + length]

    @staticmethod
    def get_string(value: bytes, pos: int) -> str:
        """Get a string value from variant value"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type == SHORT_STR:
            start = pos + 1
            length = type_info
            VariantUtil.check_index(start + length - 1, len(value))
            return value[start:start + length].decode('utf-8')
        elif basic_type == PRIMITIVE and type_info == LONG_STR:
            start = pos + 1 + U32_SIZE
            length = VariantUtil.read_unsigned(value, pos + 1, U32_SIZE)
            VariantUtil.check_index(start + length - 1, len(value))
            return value[start:start + length].decode('utf-8')
        else:
            raise VariantUtil.unexpected_type(Type.STRING)

    @staticmethod
    def get_uuid(value: bytes, pos: int) -> uuid.UUID:
        """Get a UUID value from variant value"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type != PRIMITIVE or type_info != UUID:
            raise VariantUtil.unexpected_type(Type.UUID)
        
        start = pos + 1
        VariantUtil.check_index(start + 15, len(value))
        
        # UUID values are big-endian
        return uuid.UUID(bytes=value[start:start+16])

    @staticmethod
    def handle_object(value: bytes, pos: int, handler: Callable[[int, int, int, int, int, int], T]) -> T:
        """Helper function to access a variant object"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type != OBJECT:
            raise VariantUtil.unexpected_type(Type.OBJECT)
        
        # Extract header information
        large_size = ((type_info >> 4) & 0x1) != 0
        size_bytes = U32_SIZE if large_size else 1
        size = VariantUtil.read_unsigned(value, pos + 1, size_bytes)
        
        id_size = ((type_info >> 2) & 0x3) + 1
        offset_size = (type_info & 0x3) + 1
        
        id_start = pos + 1 + size_bytes
        offset_start = id_start + size * id_size
        data_start = offset_start + (size + 1) * offset_size
        
        return handler(size, id_size, offset_size, id_start, offset_start, data_start)

    @staticmethod
    def handle_array(value: bytes, pos: int, handler: Callable[[int, int, int, int], T]) -> T:
        """Helper function to access a variant array"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & BASIC_TYPE_MASK
        type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
        
        if basic_type != ARRAY:
            raise VariantUtil.unexpected_type(Type.ARRAY)
        
        # Extract header information
        large_size = ((type_info >> 2) & 0x1) != 0
        size_bytes = U32_SIZE if large_size else 1
        size = VariantUtil.read_unsigned(value, pos + 1, size_bytes)
        
        offset_size = (type_info & 0x3) + 1
        
        offset_start = pos + 1 + size_bytes
        data_start = offset_start + (size + 1) * offset_size
        
        return handler(size, offset_size, offset_start, data_start)

    @staticmethod
    def get_metadata_key(metadata: bytes, id_val: int) -> str:
        """Get a key at `id` in the variant metadata"""
        VariantUtil.check_index(0, len(metadata))
        
        # Extract the highest 2 bits in the metadata header
        offset_size = ((metadata[0] >> 6) & 0x3) + 1
        dict_size = VariantUtil.read_unsigned(metadata, 1, offset_size)
        
        if id_val >= dict_size:
            raise malformed_variant()
        
        # Calculate offsets
        string_start = 1 + (dict_size + 2) * offset_size
        offset = VariantUtil.read_unsigned(metadata, 1 + (id_val + 1) * offset_size, offset_size)
        next_offset = VariantUtil.read_unsigned(metadata, 1 + (id_val + 2) * offset_size, offset_size)
        
        if offset > next_offset:
            raise malformed_variant()
        
        VariantUtil.check_index(string_start + next_offset - 1, len(metadata))
        
        return metadata[string_start + offset:string_start + next_offset].decode('utf-8')

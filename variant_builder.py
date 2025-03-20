import json
import decimal
import uuid
import struct
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass

from variant_util import (
    VariantUtil, Type, malformed_variant, variant_constructor_size_limit,
    PRIMITIVE, SHORT_STR, OBJECT, ARRAY, NULL, TRUE, FALSE, INT1, INT2, INT4, INT8,
    DOUBLE, DECIMAL4, DECIMAL8, DECIMAL16, DATE, TIMESTAMP, TIMESTAMP_NTZ, FLOAT,
    BINARY, LONG_STR, UUID, U8_MAX, U16_MAX, U24_MAX, U24_SIZE, U32_SIZE,
    MAX_SHORT_STR_SIZE, SIZE_LIMIT, MAX_DECIMAL4_PRECISION, MAX_DECIMAL8_PRECISION,
    MAX_DECIMAL16_PRECISION, VERSION
)

from variant import Variant

class VariantSizeLimitException(Exception):
    """Exception for variant size limit exceeded during building"""
    def __init__(self):
        super().__init__("VARIANT_SIZE_LIMIT")

class VariantDuplicateKeyException(Exception):
    """Exception for duplicate keys in variant object"""
    def __init__(self, key: str):
        super().__init__(f"VARIANT_DUPLICATE_KEY: {key}")

class VariantBuilder:
    """Build variant value and metadata by parsing JSON values"""
    
    def __init__(self, allow_duplicate_keys: bool = False):
        self.allow_duplicate_keys = allow_duplicate_keys
        self.write_buffer = bytearray(128)
        self.write_pos = 0
        self.dictionary = {}  # Map keys to monotonically increasing id
        self.dictionary_keys = []  # Store all keys in dictionary in order of id
    
    @staticmethod
    def parse_json(json_str: str, allow_duplicate_keys: bool = False) -> Variant:
        """Parse a JSON string as a Variant value"""
        builder = VariantBuilder(allow_duplicate_keys)
        builder.build_json(json.loads(json_str))
        return builder.result()
    
    def result(self) -> Variant:
        """Build the variant metadata from dictionary_keys and return the variant result"""
        num_keys = len(self.dictionary_keys)
        
        # Calculate total string size
        dictionary_string_size = sum(len(key) for key in self.dictionary_keys)
        
        # Determine bytes required per offset entry
        max_size = max(dictionary_string_size, num_keys) if num_keys > 0 else 0
        if max_size > SIZE_LIMIT:
            raise VariantSizeLimitException()
        
        offset_size = self._get_integer_size(max_size)
        
        offset_start = 1 + offset_size
        string_start = offset_start + (num_keys + 1) * offset_size
        metadata_size = string_start + dictionary_string_size
        
        if metadata_size > SIZE_LIMIT:
            raise VariantSizeLimitException()
        
        metadata = bytearray(metadata_size)
        header_byte = VERSION | ((offset_size - 1) << 6)
        VariantUtil.write_long(metadata, 0, header_byte, 1)
        VariantUtil.write_long(metadata, 1, num_keys, offset_size)
        
        current_offset = 0
        for i in range(num_keys):
            VariantUtil.write_long(metadata, offset_start + i * offset_size, current_offset, offset_size)
            key_bytes = self.dictionary_keys[i].encode('utf-8')
            metadata[string_start + current_offset:string_start + current_offset + len(key_bytes)] = key_bytes
            current_offset += len(key_bytes)
        
        VariantUtil.write_long(metadata, offset_start + num_keys * offset_size, current_offset, offset_size)
        
        return Variant(bytes(self.write_buffer[:self.write_pos]), bytes(metadata))
    
    def value_without_metadata(self) -> bytes:
        """Return the variant value only, without metadata"""
        return bytes(self.write_buffer[:self.write_pos])
    
    def append_string(self, s: str) -> None:
        """Append a string value to the variant builder"""
        text = s.encode('utf-8')
        long_str = len(text) > MAX_SHORT_STR_SIZE
        
        self._check_capacity((1 + U32_SIZE if long_str else 1) + len(text))
        
        if long_str:
            self.write_buffer[self.write_pos] = VariantUtil.primitive_header(LONG_STR)
            self.write_pos += 1
            VariantUtil.write_long(self.write_buffer, self.write_pos, len(text), U32_SIZE)
            self.write_pos += U32_SIZE
        else:
            self.write_buffer[self.write_pos] = VariantUtil.short_str_header(len(text))
            self.write_pos += 1
        
        self.write_buffer[self.write_pos:self.write_pos + len(text)] = text
        self.write_pos += len(text)
    
    def append_null(self) -> None:
        """Append a null value to the variant builder"""
        self._check_capacity(1)
        self.write_buffer[self.write_pos] = VariantUtil.primitive_header(NULL)
        self.write_pos += 1
    
    def append_boolean(self, b: bool) -> None:
        """Append a boolean value to the variant builder"""
        self._check_capacity(1)
        self.write_buffer[self.write_pos] = VariantUtil.primitive_header(TRUE if b else FALSE)
        self.write_pos += 1
    
    def append_long(self, l: int) -> None:
        """Append a long value to the variant builder"""
        self._check_capacity(1 + 8)
        
        if l == (l & 0xFF) and -128 <= l < 128:  # Fits in INT1
            self.write_buffer[self.write_pos] = VariantUtil.primitive_header(INT1)
            self.write_pos += 1
            VariantUtil.write_long(self.write_buffer, self.write_pos, l, 1)
            self.write_pos += 1
        elif l == (l & 0xFFFF) and -32768 <= l < 32768:  # Fits in INT2
            self.write_buffer[self.write_pos] = VariantUtil.primitive_header(INT2)
            self.write_pos += 1
            VariantUtil.write_long(self.write_buffer, self.write_pos, l, 2)
            self.write_pos += 2
        elif l == (l & 0xFFFFFFFF) and -2147483648 <= l < 2147483648:  # Fits in INT4
            self.write_buffer[self.write_pos] = VariantUtil.primitive_header(INT4)
            self.write_pos += 1
            VariantUtil.write_long(self.write_buffer, self.write_pos, l, 4)
            self.write_pos += 4
        else:  # INT8
            self.write_buffer[self.write_pos] = VariantUtil.primitive_header(INT8)
            self.write_pos += 1
            VariantUtil.write_long(self.write_buffer, self.write_pos, l, 8)
            self.write_pos += 8
    
    def append_double(self, d: float) -> None:
        """Append a double value to the variant builder"""
        self._check_capacity(1 + 8)
        self.write_buffer[self.write_pos] = VariantUtil.primitive_header(DOUBLE)
        self.write_pos += 1
        
        # Pack double into 8 bytes
        packed = struct.pack('<d', d)
        self.write_buffer[self.write_pos:self.write_pos + 8] = packed
        self.write_pos += 8
    
    def append_decimal(self, d: decimal.Decimal) -> None:
        """Append a decimal value to the variant builder"""
        self._check_capacity(2 + 16)
        
        # Get scale and unscaled value
        sign, digits, exp = d.as_tuple()
        scale = -exp if exp < 0 else 0
        unscaled = int(''.join(map(str, digits)))
        if sign:
            unscaled = -unscaled
        
        if scale <= MAX_DECIMAL4_PRECISION and d.adjusted() + 1 <= MAX_DECIMAL4_PRECISION:
            # Can fit in DECIMAL4
            self.write_buffer[self.write_pos] = VariantUtil.primitive_header(DECIMAL4)
            self.write_pos += 1
            self.write_buffer[self.write_pos] = scale
            self.write_pos += 1
            VariantUtil.write_long(self.write_buffer, self.write_pos, unscaled, 4)
            self.write_pos += 4
        elif scale <= MAX_DECIMAL8_PRECISION and d.adjusted() + 1 <= MAX_DECIMAL8_PRECISION:
            # Can fit in DECIMAL8
            self.write_buffer[self.write_pos] = VariantUtil.primitive_header(DECIMAL8)
            self.write_pos += 1
            self.write_buffer[self.write_pos] = scale
            self.write_pos += 1
            VariantUtil.write_long(self.write_buffer, self.write_pos, unscaled, 8)
            self.write_pos += 8
        else:
            # Use DECIMAL16
            assert scale <= MAX_DECIMAL16_PRECISION and d.adjusted() + 1 <= MAX_DECIMAL16_PRECISION
            self.write_buffer[self.write_pos] = VariantUtil.primitive_header(DECIMAL16)
            self.write_pos += 1
            self.write_buffer[self.write_pos] = scale
            self.write_pos += 1
            
            # Convert to big-endian bytes and then reverse
            unscaled_bytes = unscaled.to_bytes(16, byteorder='big', signed=True)
            for i in range(16):
                self.write_buffer[self.write_pos + i] = unscaled_bytes[15 - i]
            
            self.write_pos += 16
    
    def append_date(self, days_since_epoch: int) -> None:
        """Append a date value to the variant builder"""
        self._check_capacity(1 + 4)
        self.write_buffer[self.write_pos] = VariantUtil.primitive_header(DATE)
        self.write_pos += 1
        VariantUtil.write_long(self.write_buffer, self.write_pos, days_since_epoch, 4)
        self.write_pos += 4
    
    def append_timestamp(self, micros_since_epoch: int) -> None:
        """Append a timestamp value to the variant builder"""
        self._check_capacity(1 + 8)
        self.write_buffer[self.write_pos] = VariantUtil.primitive_header(TIMESTAMP)
        self.write_pos += 1
        VariantUtil.write_long(self.write_buffer, self.write_pos, micros_since_epoch, 8)
        self.write_pos += 8
    
    def append_timestamp_ntz(self, micros_since_epoch: int) -> None:
        """Append a timestamp_ntz value to the variant builder"""
        self._check_capacity(1 + 8)
        self.write_buffer[self.write_pos] = VariantUtil.primitive_header(TIMESTAMP_NTZ)
        self.write_pos += 1
        VariantUtil.write_long(self.write_buffer, self.write_pos, micros_since_epoch, 8)
        self.write_pos += 8
    
    def append_float(self, f: float) -> None:
        """Append a float value to the variant builder"""
        self._check_capacity(1 + 4)
        self.write_buffer[self.write_pos] = VariantUtil.primitive_header(FLOAT)
        self.write_pos += 1
        
        # Pack float into 4 bytes
        packed = struct.pack('<f', f)
        self.write_buffer[self.write_pos:self.write_pos + 4] = packed
        self.write_pos += 4
    
    def append_binary(self, binary: bytes) -> None:
        """Append a binary value to the variant builder"""
        self._check_capacity(1 + U32_SIZE + len(binary))
        self.write_buffer[self.write_pos] = VariantUtil.primitive_header(BINARY)
        self.write_pos += 1
        VariantUtil.write_long(self.write_buffer, self.write_pos, len(binary), U32_SIZE)
        self.write_pos += U32_SIZE
        self.write_buffer[self.write_pos:self.write_pos + len(binary)] = binary
        self.write_pos += len(binary)
    
    def append_uuid(self, uuid_val: uuid.UUID) -> None:
        """Append a UUID value to the variant builder"""
        self._check_capacity(1 + 16)
        self.write_buffer[self.write_pos] = VariantUtil.primitive_header(UUID)
        self.write_pos += 1
        
        # UUID is stored big-endian
        uuid_bytes = uuid_val.bytes
        self.write_buffer[self.write_pos:self.write_pos + 16] = uuid_bytes
        self.write_pos += 16
    
    def add_key(self, key: str) -> int:
        """Add a key to the variant dictionary"""
        if key in self.dictionary:
            return self.dictionary[key]
        else:
            id_val = len(self.dictionary_keys)
            self.dictionary[key] = id_val
            self.dictionary_keys.append(key)
            return id_val
    
    def get_write_pos(self) -> int:
        """Return the current write position of the variant builder"""
        return self.write_pos
    
    @dataclass
    class FieldEntry:
        """Store information about a field in an object"""
        key: str
        id: int
        offset: int
        
        def with_new_offset(self, new_offset: int) -> 'VariantBuilder.FieldEntry':
            return VariantBuilder.FieldEntry(self.key, self.id, new_offset)
    
    def finish_writing_object(self, start: int, fields: List[FieldEntry]) -> None:
        """Finish writing a variant object after all fields have been written"""
        size = len(fields)
        
        # Sort fields by key
        fields.sort(key=lambda f: f.key)
        
        max_id = max([f.id for f in fields]) if fields else 0
        
        if self.allow_duplicate_keys:
            # Handle duplicate keys - keep the field with the greatest offset
            distinct_fields = []
            i = 0
            while i < size:
                current_key = fields[i].key
                max_offset_field = fields[i]
                
                # Find all fields with the same key and keep the one with max offset
                j = i + 1
                while j < size and fields[j].key == current_key:
                    if fields[j].offset > max_offset_field.offset:
                        max_offset_field = fields[j]
                    j += 1
                
                distinct_fields.append(max_offset_field)
                i = j
            
            if len(distinct_fields) < size:
                # We had duplicates, need to reorganize the data
                fields = distinct_fields
                size = len(fields)
                
                # Sort by offsets to move data without overwriting
                fields.sort(key=lambda f: f.offset)
                
                current_offset = 0
                for i in range(size):
                    old_offset = fields[i].offset
                    field_size = VariantUtil.value_size(self.write_buffer, start + old_offset)
                    
                    # Move the field data
                    if current_offset != old_offset:
                        self.write_buffer[start + current_offset:start + current_offset + field_size] = \
                            self.write_buffer[start + old_offset:start + old_offset + field_size]
                    
                    fields[i] = fields[i].with_new_offset(current_offset)
                    current_offset += field_size
                
                self.write_pos = start + current_offset
                
                # Sort back by key
                fields.sort(key=lambda f: f.key)
        else:
            # Check for duplicate keys
            for i in range(1, size):
                if fields[i].key == fields[i-1].key:
                    raise VariantDuplicateKeyException(fields[i].key)
        
        data_size = self.write_pos - start
        large_size = size > U8_MAX
        size_bytes = U32_SIZE if large_size else 1
        id_size = self._get_integer_size(max_id)
        offset_size = self._get_integer_size(data_size)
        
        # Space for header byte, object size, id list, and offset list
        header_size = 1 + size_bytes + size * id_size + (size + 1) * offset_size
        self._check_capacity(header_size)
        
        # Shift the field data to make room for the object header
        for i in range(data_size - 1, -1, -1):
            self.write_buffer[start + header_size + i] = self.write_buffer[start + i]
        
        self.write_pos += header_size
        
        # Write the object header
        self.write_buffer[start] = VariantUtil.object_header(large_size, id_size, offset_size)
        VariantUtil.write_long(self.write_buffer, start + 1, size, size_bytes)
        
        id_start = start + 1 + size_bytes
        offset_start = id_start + size * id_size
        
        # Write field IDs and offsets
        for i in range(size):
            VariantUtil.write_long(self.write_buffer, id_start + i * id_size, fields[i].id, id_size)
            VariantUtil.write_long(self.write_buffer, offset_start + i * offset_size, fields[i].offset, offset_size)
        
        # Write the total data size as the last offset
        VariantUtil.write_long(self.write_buffer, offset_start + size * offset_size, data_size, offset_size)
    
    def finish_writing_array(self, start: int, offsets: List[int]) -> None:
        """Finish writing a variant array after all elements have been written"""
        size = len(offsets)
        data_size = self.write_pos - start
        
        large_size = size > U8_MAX
        size_bytes = U32_SIZE if large_size else 1
        offset_size = self._get_integer_size(data_size)
        
        # Space for header byte, array size, and offset list
        header_size = 1 + size_bytes + (size + 1) * offset_size
        self._check_capacity(header_size)
        
        # Shift the element data to make room for the header
        for i in range(data_size - 1, -1, -1):
            self.write_buffer[start + header_size + i] = self.write_buffer[start + i]
        
        self.write_pos += header_size
        
        # Write the array header
        self.write_buffer[start] = VariantUtil.array_header(large_size, offset_size)
        VariantUtil.write_long(self.write_buffer, start + 1, size, size_bytes)
        
        offset_start = start + 1 + size_bytes
        
        # Write element offsets
        for i in range(size):
            VariantUtil.write_long(self.write_buffer, offset_start + i * offset_size, offsets[i], offset_size)
        
        # Write the total data size as the last offset
        VariantUtil.write_long(self.write_buffer, offset_start + size * offset_size, data_size, offset_size)
    
    def append_variant(self, v: Variant) -> None:
        """Append a variant value to the variant builder"""
        self._append_variant_impl(v.value, v.metadata, v.pos)
    
    def _append_variant_impl(self, value: bytes, metadata: bytes, pos: int) -> None:
        """Implementation of append_variant"""
        VariantUtil.check_index(pos, len(value))
        basic_type = value[pos] & 0x3  # BASIC_TYPE_MASK
        
        if basic_type == OBJECT:
            VariantUtil.handle_object(
                value, pos,
                lambda size, id_size, offset_size, id_start, offset_start, data_start: 
                    self._append_object(
                        value, metadata, size, id_size, offset_size, id_start, 
                        offset_start, data_start
                    )
            )
        elif basic_type == ARRAY:
            VariantUtil.handle_array(
                value, pos,
                lambda size, offset_size, offset_start, data_start: 
                    self._append_array(
                        value, metadata, size, offset_size, offset_start, data_start
                    )
            )
        else:
            self._shallow_append_variant_impl(value, pos)
    
    def _append_object(self, value, metadata, size, id_size, offset_size, id_start, offset_start, data_start):
        """Helper to append an object variant"""
        fields = []
        start = self.write_pos
        
        for i in range(size):
            id_val = VariantUtil.read_unsigned(value, id_start + id_size * i, id_size)
            offset = VariantUtil.read_unsigned(value, offset_start + offset_size * i, offset_size)
            element_pos = data_start + offset
            
            key = VariantUtil.get_metadata_key(metadata, id_val)
            new_id = self.add_key(key)
            
            fields.append(self.FieldEntry(key, new_id, self.write_pos - start))
            self._append_variant_impl(value, metadata, element_pos)
        
        self.finish_writing_object(start, fields)
        return None
    
    def _append_array(self, value, metadata, size, offset_size, offset_start, data_start):
        """Helper to append an array variant"""
        offsets = []
        start = self.write_pos
        
        for i in range(size):
            offset = VariantUtil.read_unsigned(value, offset_start + offset_size * i, offset_size)
            element_pos = data_start + offset
            
            offsets.append(self.write_pos - start)
            self._append_variant_impl(value, metadata, element_pos)
        
        self.finish_writing_array(start, offsets)
        return None
    
    def shallow_append_variant(self, v: Variant) -> None:
        """Append variant without rewriting or creating metadata"""
        self._shallow_append_variant_impl(v.value, v.pos)
    
    def _shallow_append_variant_impl(self, value: bytes, pos: int) -> None:
        """Implementation of shallow_append_variant"""
        size = VariantUtil.value_size(value, pos)
        VariantUtil.check_index(pos + size - 1, len(value))
        
        self._check_capacity(size)
        self.write_buffer[self.write_pos:self.write_pos + size] = value[pos:pos + size]
        self.write_pos += size
    
    def _check_capacity(self, additional: int) -> None:
        """Ensure the write buffer has enough capacity"""
        required = self.write_pos + additional
        
        if required > len(self.write_buffer):
            # Allocate a new buffer with capacity of next power of 2
            new_capacity = 1
            while new_capacity < required:
                new_capacity *= 2
            
            if new_capacity > SIZE_LIMIT:
                raise VariantSizeLimitException()
            
            new_buffer = bytearray(new_capacity)
            new_buffer[:self.write_pos] = self.write_buffer[:self.write_pos]
            self.write_buffer = new_buffer
    
    def _get_integer_size(self, value: int) -> int:
        """Choose the smallest unsigned integer type that can store `value`"""
        assert 0 <= value <= U24_MAX
        
        if value <= U8_MAX:
            return 1
        elif value <= U16_MAX:
            return 2
        else:
            return U24_SIZE
    
    def build_json(self, json_data: Any) -> None:
        """Build a variant from a Python object (parsed JSON)"""
        if isinstance(json_data, dict):
            # Handle object
            fields = []
            start = self.write_pos
            
            for key, value in json_data.items():
                id_val = self.add_key(key)
                fields.append(self.FieldEntry(key, id_val, self.write_pos - start))
                self.build_json(value)
            
            self.finish_writing_object(start, fields)
        
        elif isinstance(json_data, list):
            # Handle array
            offsets = []
            start = self.write_pos
            
            for item in json_data:
                offsets.append(self.write_pos - start)
                self.build_json(item)
            
            self.finish_writing_array(start, offsets)
        
        elif isinstance(json_data, str):
            self.append_string(json_data)

        elif isinstance(json_data, bool):
            self.append_boolean(json_data)

        elif isinstance(json_data, float):
            # Try to parse as decimal first
            try:
                d = decimal.Decimal(str(json_data))
                if (d.as_tuple().exponent >= -MAX_DECIMAL16_PRECISION and 
                    len(d.as_tuple().digits) <= MAX_DECIMAL16_PRECISION):
                    self.append_decimal(d)
                    return
            except:
                pass
            
            # Fall back to double
            self.append_double(json_data)

        elif isinstance(json_data, int):
            self.append_long(json_data)

        elif json_data is None:
            self.append_null()
        
        else:
            raise ValueError(f"Unsupported JSON type: {type(json_data)}")

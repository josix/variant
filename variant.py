import json
import base64
import uuid
import decimal
import datetime
from typing import Optional
from dataclasses import dataclass

from variant_util import (
    VariantUtil, Type, malformed_variant, variant_constructor_size_limit,
    SIZE_LIMIT, VERSION, VERSION_MASK
)

class Variant:
    """
    Python equivalent of the Java Variant class.
    This class is structurally equivalent to the Java version.
    """
    def __init__(self, value: bytes, metadata: bytes, pos: int = 0):
        self.value = value
        self.metadata = metadata
        self.pos = pos
        
        # There is currently only one allowed version.
        if len(metadata) < 1 or (metadata[0] & VERSION_MASK) != VERSION:
            raise malformed_variant()
            
        # Don't attempt to use a Variant larger than 16 MiB. We'll never produce one, and it risks
        # memory instability.
        if len(metadata) > SIZE_LIMIT or len(value) > SIZE_LIMIT:
            raise variant_constructor_size_limit()

    def get_value(self) -> bytes:
        if self.pos == 0:
            return self.value
        size = VariantUtil.value_size(self.value, self.pos)
        VariantUtil.check_index(self.pos + size - 1, len(self.value))
        return self.value[self.pos:self.pos + size]

    def get_metadata(self) -> bytes:
        return self.metadata

    # Get a boolean value from the variant.
    def get_boolean(self) -> bool:
        return VariantUtil.get_boolean(self.value, self.pos)

    # Get a long value from the variant.
    def get_long(self) -> int:
        return VariantUtil.get_long(self.value, self.pos)

    # Get a double value from the variant.
    def get_double(self) -> float:
        return VariantUtil.get_double(self.value, self.pos)

    # Get a decimal value from the variant.
    def get_decimal(self) -> decimal.Decimal:
        return VariantUtil.get_decimal(self.value, self.pos)

    # Get a float value from the variant.
    def get_float(self) -> float:
        return VariantUtil.get_float(self.value, self.pos)

    # Get a binary value from the variant.
    def get_binary(self) -> bytes:
        return VariantUtil.get_binary(self.value, self.pos)

    # Get a string value from the variant.
    def get_string(self) -> str:
        return VariantUtil.get_string(self.value, self.pos)

    # Get the type info bits from a variant value.
    def get_type_info(self) -> int:
        return VariantUtil.get_type_info(self.value, self.pos)

    # Get the value type of the variant.
    def get_type(self) -> Type:
        return VariantUtil.get_type(self.value, self.pos)

    # Get a UUID value from the variant.
    def get_uuid(self) -> uuid.UUID:
        return VariantUtil.get_uuid(self.value, self.pos)

    # Get the number of object fields in the variant.
    # It is only legal to call it when `get_type()` is `Type.OBJECT`.
    def object_size(self) -> int:
        return VariantUtil.handle_object(
            self.value, self.pos,
            lambda size, id_size, offset_size, id_start, offset_start, data_start: size
        )

    # Find the field value whose key is equal to `key`. Return None if the key is not found.
    # It is only legal to call it when `get_type()` is `Type.OBJECT`.
    def get_field_by_key(self, key: str):
        return VariantUtil.handle_object(
            self.value, self.pos,
            lambda size, id_size, offset_size, id_start, offset_start, data_start: self._find_field(
                key, size, id_size, offset_size, id_start, offset_start, data_start
            )
        )
    
    def _find_field(self, key, size, id_size, offset_size, id_start, offset_start, data_start):
        # Use linear search for a short list. Switch to binary search when the length reaches
        # `BINARY_SEARCH_THRESHOLD`.
        BINARY_SEARCH_THRESHOLD = 32
        if size < BINARY_SEARCH_THRESHOLD:
            for i in range(size):
                id_val = VariantUtil.read_unsigned(self.value, id_start + id_size * i, id_size)
                if key == VariantUtil.get_metadata_key(self.metadata, id_val):
                    offset = VariantUtil.read_unsigned(self.value, offset_start + offset_size * i, offset_size)
                    return Variant(self.value, self.metadata, data_start + offset)
        else:
            low = 0
            high = size - 1
            while low <= high:
                # Use unsigned right shift to compute the middle of `low` and `high`.
                mid = (low + high) >> 1
                id_val = VariantUtil.read_unsigned(self.value, id_start + id_size * mid, id_size)
                cmp = VariantUtil.get_metadata_key(self.metadata, id_val).compare_to(key)
                if cmp < 0:
                    low = mid + 1
                elif cmp > 0:
                    high = mid - 1
                else:
                    offset = VariantUtil.read_unsigned(self.value, offset_start + offset_size * mid, offset_size)
                    return Variant(self.value, self.metadata, data_start + offset)
        return None

    @dataclass
    class ObjectField:
        key: str
        value: 'Variant'

    # Get the object field at the `index` slot. Return None if `index` is out of the bound of
    # `[0, object_size())`.
    # It is only legal to call it when `get_type()` is `Type.OBJECT`.
    def get_field_at_index(self, index: int) -> Optional['Variant.ObjectField']:
        return VariantUtil.handle_object(
            self.value, self.pos,
            lambda size, id_size, offset_size, id_start, offset_start, data_start: self._get_field_at_index(
                index, size, id_size, offset_size, id_start, offset_start, data_start
            )
        )
    
    def _get_field_at_index(self, index, size, id_size, offset_size, id_start, offset_start, data_start):
        if index < 0 or index >= size:
            return None
        id_val = VariantUtil.read_unsigned(self.value, id_start + id_size * index, id_size)
        offset = VariantUtil.read_unsigned(self.value, offset_start + offset_size * index, offset_size)
        key = VariantUtil.get_metadata_key(self.metadata, id_val)
        v = Variant(self.value, self.metadata, data_start + offset)
        return Variant.ObjectField(key, v)

    # Get the dictionary ID for the object field at the `index` slot. Throws malformed_variant if
    # `index` is out of the bound of `[0, object_size())`.
    # It is only legal to call it when `get_type()` is `Type.OBJECT`.
    def get_dictionary_id_at_index(self, index: int) -> int:
        return VariantUtil.handle_object(
            self.value, self.pos,
            lambda size, id_size, offset_size, id_start, offset_start, data_start: self._get_dict_id_at_index(
                index, size, id_size, offset_size, id_start, offset_start, data_start
            )
        )
    
    def _get_dict_id_at_index(self, index, size, id_size, offset_size, id_start, offset_start, data_start):
        if index < 0 or index >= size:
            raise malformed_variant()
        return VariantUtil.read_unsigned(self.value, id_start + id_size * index, id_size)

    # Get the number of array elements in the variant.
    # It is only legal to call it when `get_type()` is `Type.ARRAY`.
    def array_size(self) -> int:
        return VariantUtil.handle_array(
            self.value, self.pos,
            lambda size, offset_size, offset_start, data_start: size
        )

    # Get the array element at the `index` slot. Return None if `index` is out of the bound of
    # `[0, array_size())`.
    # It is only legal to call it when `get_type()` is `Type.ARRAY`.
    def get_element_at_index(self, index: int) -> Optional['Variant']:
        return VariantUtil.handle_array(
            self.value, self.pos,
            lambda size, offset_size, offset_start, data_start: self._get_element_at_index(
                index, size, offset_size, offset_start, data_start
            )
        )
    
    def _get_element_at_index(self, index, size, offset_size, offset_start, data_start):
        if index < 0 or index >= size:
            return None
        offset = VariantUtil.read_unsigned(self.value, offset_start + offset_size * index, offset_size)
        return Variant(self.value, self.metadata, data_start + offset)

    # Stringify the variant in JSON format.
    # Throw `MALFORMED_VARIANT` if the variant is malformed.
    def to_json(self, zone_id=None) -> str:
        result = []
        self._to_json_impl(self.value, self.metadata, self.pos, result, zone_id)
        return ''.join(result)

    @staticmethod
    def _to_json_impl(value, metadata, pos, result, zone_id):
        variant_type = VariantUtil.get_type(value, pos)
        
        if variant_type == Type.OBJECT:
            VariantUtil.handle_object(
                value, pos,
                lambda size, id_size, offset_size, id_start, offset_start, data_start: 
                    Variant._handle_object_json(
                        value, metadata, size, id_size, offset_size, id_start, 
                        offset_start, data_start, result, zone_id
                    )
            )
        elif variant_type == Type.ARRAY:
            VariantUtil.handle_array(
                value, pos,
                lambda size, offset_size, offset_start, data_start: 
                    Variant._handle_array_json(
                        value, metadata, size, offset_size, offset_start, 
                        data_start, result, zone_id
                    )
            )
        elif variant_type == Type.NULL:
            result.append("null")
        elif variant_type == Type.BOOLEAN:
            result.append(str(VariantUtil.get_boolean(value, pos)).lower())
        elif variant_type == Type.LONG:
            result.append(str(VariantUtil.get_long(value, pos)))
        elif variant_type == Type.STRING:
            result.append(json.dumps(VariantUtil.get_string(value, pos)))
        elif variant_type == Type.DOUBLE:
            result.append(str(VariantUtil.get_double(value, pos)))
        elif variant_type == Type.DECIMAL:
            result.append(str(VariantUtil.get_decimal(value, pos)))
        elif variant_type == Type.DATE:
            date_val = datetime.date.fromordinal(int(VariantUtil.get_long(value, pos)) + 719163)  # Adjust for epoch
            result.append(f'"{date_val.isoformat()}"')
        elif variant_type == Type.TIMESTAMP:
            micros = VariantUtil.get_long(value, pos)
            dt = datetime.datetime.fromtimestamp(micros / 1_000_000, tz=zone_id or datetime.timezone.utc)
            result.append(f'"{dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}{dt.strftime("%z")}"')
        elif variant_type == Type.TIMESTAMP_NTZ:
            micros = VariantUtil.get_long(value, pos)
            dt = datetime.datetime.fromtimestamp(micros / 1_000_000, tz=datetime.timezone.utc)
            result.append(f'"{dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}"')
        elif variant_type == Type.FLOAT:
            result.append(str(VariantUtil.get_float(value, pos)))
        elif variant_type == Type.BINARY:
            binary_data = VariantUtil.get_binary(value, pos)
            result.append(f'"{base64.b64encode(binary_data).decode("ascii")}"')
        elif variant_type == Type.UUID:
            result.append(f'"{VariantUtil.get_uuid(value, pos)}"')

    @staticmethod
    def _handle_object_json(value, metadata, size, id_size, offset_size, id_start, offset_start, data_start, result, zone_id):
        result.append('{')
        for i in range(size):
            id_val = VariantUtil.read_unsigned(value, id_start + id_size * i, id_size)
            offset = VariantUtil.read_unsigned(value, offset_start + offset_size * i, offset_size)
            element_pos = data_start + offset
            if i != 0:
                result.append(',')
            result.append(json.dumps(VariantUtil.get_metadata_key(metadata, id_val)))
            result.append(':')
            Variant._to_json_impl(value, metadata, element_pos, result, zone_id)
        result.append('}')
        return None

    @staticmethod
    def _handle_array_json(value, metadata, size, offset_size, offset_start, data_start, result, zone_id):
        result.append('[')
        for i in range(size):
            offset = VariantUtil.read_unsigned(value, offset_start + offset_size * i, offset_size)
            element_pos = data_start + offset
            if i != 0:
                result.append(',')
            Variant._to_json_impl(value, metadata, element_pos, result, zone_id)
        result.append(']')
        return None

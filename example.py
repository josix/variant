"""
Example usage of the Variant Python implementation.
"""

import sys
import json
from variant_builder import VariantBuilder

def main():
    # Create a variant from JSON
    json_data = {
        "name": "John Doe",
        "age": 30,
        "is_active": True,
        "scores": [95.5, 87.0, 92.3],
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "zip": "12345"
        },
        "tags": ["developer", "python", "data"]
    }
    
    # Convert to JSON string
    json_str = json.dumps(json_data)
    print(f"Original JSON: {json_str}")
    
    # Parse JSON into a Variant
    
    print(sys.getsizeof(json_str))
    variant = VariantBuilder.parse_json(json_str)
    print(sys.getsizeof(variant))
    
    # Convert Variant back to JSON
    json_output = variant.to_json()
    print(f"Variant to JSON: {json_output}")
    
    # Access specific fields
    name = variant.get_field_by_key("name").get_string()
    age = variant.get_field_by_key("age").get_long()
    is_active = variant.get_field_by_key("is_active").get_boolean()
    
    print(f"Name: {name}")
    print(f"Age: {age}")
    print(f"Is Active: {is_active}")
    
    # Access array elements
    scores = variant.get_field_by_key("scores")
    print(f"Number of scores: {scores.array_size()}")
    for i in range(scores.array_size()):
        score = scores.get_element_at_index(i).get_double()
        print(f"Score {i+1}: {score}")
    
    # Access nested object
    address = variant.get_field_by_key("address")
    street = address.get_field_by_key("street").get_string()
    city = address.get_field_by_key("city").get_string()
    zip_code = address.get_field_by_key("zip").get_string()
    
    print(f"Address: {street}, {city}, {zip_code}")
    
    # Create a variant manually
    builder = VariantBuilder(False)
    
    # Start building an object
    obj_start = builder.get_write_pos()
    fields = []
    
    # Add a string field
    key = "name"
    id_val = builder.add_key(key)
    fields.append(VariantBuilder.FieldEntry(key, id_val, builder.get_write_pos() - obj_start))
    builder.append_string("Jane Smith")
    
    # Add a numeric field
    key = "age"
    id_val = builder.add_key(key)
    fields.append(VariantBuilder.FieldEntry(key, id_val, builder.get_write_pos() - obj_start))
    builder.append_long(28)
    
    # Add a boolean field
    key = "is_student"
    id_val = builder.add_key(key)
    fields.append(VariantBuilder.FieldEntry(key, id_val, builder.get_write_pos() - obj_start))
    builder.append_boolean(True)
    
    # Add an array field
    key = "hobbies"
    id_val = builder.add_key(key)
    fields.append(VariantBuilder.FieldEntry(key, id_val, builder.get_write_pos() - obj_start))
    
    # Start building an array
    array_start = builder.get_write_pos()
    array_offsets = []
    
    # Add array elements
    array_offsets.append(builder.get_write_pos() - array_start)
    builder.append_string("reading")
    
    array_offsets.append(builder.get_write_pos() - array_start)
    builder.append_string("hiking")
    
    array_offsets.append(builder.get_write_pos() - array_start)
    builder.append_string("coding")
    
    # Finish the array
    builder.finish_writing_array(array_start, array_offsets)
    
    # Finish the object
    builder.finish_writing_object(obj_start, fields)
    
    # Get the final variant
    manual_variant = builder.result()
    
    # Convert to JSON and print
    manual_json = manual_variant.to_json()
    print(f"\nManually created variant: {manual_json}")

if __name__ == "__main__":
    main()

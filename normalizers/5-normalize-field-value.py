import json
import sys
import os

def normalize_field_value(normalization_map_path, data_path, output_path):
	"""
	Normalize field values in a JSON array using a normalization map.
	Args:
		normalization_map_path (str): Path to the normalization map JSON file.
		data_path (str): Path to the input data JSON file (array of objects).
		output_path (str): Path to write the normalized output JSON file.
	"""
	# Load normalization map
	with open(normalization_map_path, 'r', encoding='utf-8') as f:
		normalization_map = json.load(f)

	# Load data
	with open(data_path, 'r', encoding='utf-8') as f:
		data = json.load(f)

	normalized_data = []
	for item in data:
		new_item = item.copy()
		for field, value in item.items():
			if field in normalization_map:
				field_map = normalization_map[field]
				# Normalize lookup_key for null and boolean values
				if value is None:
					lookup_key = "null"
				elif isinstance(value, bool):
					lookup_key = str(value).lower()  # "true" or "false"
				else:
					lookup_key = str(value)
				# If value not in map, keep original
				if lookup_key in field_map:
					normalized_value = field_map[lookup_key]
					# Convert string 'true'/'false'/'null' to actual types
					if normalized_value == "true":
						new_item[field] = True
					elif normalized_value == "false":
						new_item[field] = False
					elif normalized_value == "null":
						new_item[field] = None
					else:
						new_item[field] = normalized_value
		normalized_data.append(new_item)

	# Write output
	with open(output_path, 'w', encoding='utf-8') as f:
		json.dump(normalized_data, f, indent=2, ensure_ascii=False)

	print(f"Normalized {len(normalized_data)} items. Output written to {output_path}")


if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: python 5-normalize-field-value.py <normalization_map_path> <data_path> <output_path>")
		sys.exit(1)
	normalization_map_path = sys.argv[1]
	data_path = sys.argv[2]
	output_path = sys.argv[3]
	normalize_field_value(normalization_map_path, data_path, output_path)

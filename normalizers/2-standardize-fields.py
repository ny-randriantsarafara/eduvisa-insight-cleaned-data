import sys
import os
import json

def standardize_fields(fields_path, data_path, output_path):
	"""
	Add missing fields and remove non-existing ones from each object in the data file, using the fields from fields_path.
	Args:
		fields_path (str): Path to JSON file containing a list of field names.
		data_path (str): Path to JSON file containing a list of objects.
		output_path (str): Path to write the output JSON file.
	"""
	# Load fields
	with open(fields_path, 'r', encoding='utf-8') as f:
		fields = json.load(f)
	if not isinstance(fields, list):
		raise ValueError(f"{fields_path} does not contain a JSON array of field names.")

	# Load data
	with open(data_path, 'r', encoding='utf-8') as f:
		data = json.load(f)
	if not isinstance(data, list):
		raise ValueError(f"{data_path} does not contain a JSON array of objects.")

	# Create new objects with only the specified fields
	new_data = []
	for obj in data:
		if isinstance(obj, dict):
			new_obj = {field: obj.get(field, None) for field in fields}
			new_data.append(new_obj)
		else:
			new_data.append(obj)

	# Write output
	with open(output_path, 'w', encoding='utf-8') as out_f:
		json.dump(new_data, out_f, indent=2, ensure_ascii=False)
	print(f"Wrote {len(new_data)} objects with only specified fields to {output_path}")


if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: python 2-standardize-fields.py <fields.json> <data.json> <output.json>")
		sys.exit(1)
	standardize_fields(sys.argv[1], sys.argv[2], sys.argv[3])

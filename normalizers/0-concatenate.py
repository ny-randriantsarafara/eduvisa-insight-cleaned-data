import os
import sys
import json

def concatenate_json_files(input_files, output_file="concatenated.json"):
	"""
	Concatenate JSON arrays from multiple files into a single JSON array file.
	Args:
		input_files (list of str): List of input JSON file paths.
		output_file (str): Output file path (default: 'concatenated.json').
	Returns:
		int: Number of objects written to the output file.
	"""
	all_items = []
	for file_path in input_files:
		abs_path = os.path.abspath(file_path)
		print(f"Processing file: {abs_path}")
		if not os.path.isfile(abs_path):
			print(f"File not found: {abs_path}")
			continue
		with open(abs_path, 'r', encoding='utf-8') as f:
			try:
				data = json.load(f)
				if not isinstance(data, list):
					print(f"File {abs_path} does not contain a JSON array. Skipping.")
					continue
				all_items.extend(data)
			except Exception as e:
				print(f"Error reading {abs_path}: {e}")
				continue
	with open(output_file, 'w', encoding='utf-8') as out_f:
		json.dump(all_items, out_f, indent=2, ensure_ascii=False)
	print(f"Wrote {len(all_items)} items to {output_file}")
	return len(all_items)


if __name__ == "__main__":
	if len(sys.argv) < 2:
		print("Usage: python concatenate.py <json_file1> <json_file2> ... [output_file]")
		sys.exit(1)
	args = sys.argv[1:]
	if len(args) >= 2 and args[-1].endswith('.json') and os.path.splitext(args[-1])[0] != '':
		# If the last argument is a .json file and not just an empty string, treat as output file
		input_files = args[:-1]
		output_file = args[-1]
	else:
		input_files = args
		output_file = "concatenated.json"
	concatenate_json_files(input_files, output_file)

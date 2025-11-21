import sys
import os
import json

def collect_field_values(input_file, output_file):
    """
    Reads a JSON array file, collects all unique values for each field across all objects,
    and writes the result as a dictionary to the output file.
    Args:
        input_file (str): Path to the input JSON array file.
        output_file (str): Path to write the output JSON dictionary file.
    """
    if not os.path.isfile(input_file):
        print(f"File not found: {input_file}")
        return
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, list):
        print(f"Input file does not contain a JSON array.")
        return
    field_values = {}
    for obj in data:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k not in field_values:
                    field_values[k] = set()
                field_values[k].add(json.dumps(v, ensure_ascii=False, sort_keys=True))
    # Convert sets to lists and decode JSON values, then sort with a key that handles None and mixed types
    def safe_sort_key(x):
        if x is None:
            return (0, '')
        return (1, str(x))
    result = {k: sorted([json.loads(val) for val in vals], key=safe_sort_key) for k, vals in field_values.items()}
    with open(output_file, 'w', encoding='utf-8') as out_f:
        json.dump(result, out_f, indent=2, ensure_ascii=False)
    print(f"Extracted field values for {len(result)} fields to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python 3-extract-field-values.py <input.json> <output.json>")
        sys.exit(1)
    collect_field_values(sys.argv[1], sys.argv[2])

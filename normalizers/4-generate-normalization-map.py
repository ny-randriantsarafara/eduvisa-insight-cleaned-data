import json
import os
import sys

def main():
    if len(sys.argv) != 3:
        print("Usage: python 4-generate-normalization-map.py <input.json> <output.json>")
        sys.exit(1)
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    if not os.path.isfile(input_path):
        print(f"File not found: {input_path}")
        sys.exit(1)
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    normalization_map = {}
    for field, values in data.items():
        value_map = {}
        for value in values:
            value_map[value] = ""  # Always use empty string as replacement
        normalization_map[field] = value_map

    with open(output_path, 'w', encoding='utf-8') as out_f:
        json.dump(normalization_map, out_f, ensure_ascii=False, indent=2)
    print(f"Normalization map written to {output_path}")

if __name__ == '__main__':
    main()

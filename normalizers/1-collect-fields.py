import os
import json



def collect_fields_from_json_files(relative_paths, output_path):
    all_fields = set()
    total_files = 0
    total_objects = 0

    for rel_path in relative_paths:
        abs_path = os.path.abspath(rel_path)
        print(f"Processing file: {abs_path}")
        if not os.path.isfile(abs_path):
            print(f"File not found: {abs_path}")
            continue

        with open(abs_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                if not isinstance(data, list):
                    print(f"File {abs_path} does not contain a JSON array.")
                    continue
            except Exception as e:
                print(f"Error reading {abs_path}: {e}")
                continue

            total_files += 1
            for obj in data:
                if isinstance(obj, dict):
                    all_fields.update(obj.keys())
                    total_objects += 1

    result = sorted(list(all_fields))
    print(f"\nProcessed {total_files} files and {total_objects} objects.")
    print(f"Total unique fields found: {len(result)}")

    try:
        with open(output_path, 'w', encoding='utf-8') as out_f:
            json.dump(result, out_f, indent=2, ensure_ascii=False)
        print(f"Fields written to {output_path}")
    except Exception as e:
        print(f"Failed to write to {output_path}: {e}")

    return result


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python 1-collect-fields.py <json_file1> <json_file2> ... <output_file>")
    else:
        input_files = sys.argv[1:-1]
        output_file = sys.argv[-1]
        collect_fields_from_json_files(input_files, output_file)

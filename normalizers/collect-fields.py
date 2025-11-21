import os
import json


def collect_fields_from_json_files(relative_paths):
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


    output_path = "fields.json"
    try:
        with open(output_path, 'w', encoding='utf-8') as out_f:
            json.dump(result, out_f, indent=2)
        print(f"Fields written to {output_path}")
    except Exception as e:
        print(f"Failed to write to {output_path}: {e}")

    return result


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python collect-fields.py <json_file1> <json_file2> ...")
    else:
        collect_fields_from_json_files(sys.argv[1:])

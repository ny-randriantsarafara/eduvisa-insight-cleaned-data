import json
import hashlib
import sys

# Usage: python 6-generate-id.py <input.json> <output.json>
def generate_id(obj):
    # Exclude 'id' field if present
    obj_copy = dict(obj)
    obj_copy.pop('id', None)
    # Serialize object with sorted keys for deterministic output
    obj_str = json.dumps(obj_copy, sort_keys=True, separators=(",", ":"))
    # Hash the string
    hash_hex = hashlib.sha256(obj_str.encode("utf-8")).hexdigest()
    # Format the whole hash as a UUID: 8-4-4-4-12-remaining
    uuid_like = f"{hash_hex[:8]}-{hash_hex[8:12]}-{hash_hex[12:16]}-{hash_hex[16:20]}-{hash_hex[20:32]}-{hash_hex[32:64]}"
    return uuid_like

def main():
    if len(sys.argv) != 3:
        print("Usage: python 6-generate-id.py <input.json> <output.json>")
        sys.exit(1)
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    print(f"Reading input file: {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    count = 0
    # If the root is a list, process each object
    if isinstance(data, list):
        for obj in data:
            obj["id"] = generate_id(obj)
            count += 1
    # If the root is a dict, process each value if it's a list
    elif isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, list):
                for obj in v:
                    obj["id"] = generate_id(obj)
                    count += 1
    else:
        print("Unsupported JSON structure.")
        sys.exit(1)
    print(f"Processed {count} objects. Writing output file: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Done.")

if __name__ == "__main__":
    main()

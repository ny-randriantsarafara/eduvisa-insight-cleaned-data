import os
import sys
import json
import hashlib
import re

def concatenate_json_files(input_files, output_file):
    """
    Concatenate JSON arrays from multiple files into a single JSON array file.
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

def standardize_fields(fields, data_path, output_path):
    """
    Add missing fields and remove non-existing ones from each object in the data file, using the fields from fields_path.
    """
    if not isinstance(fields, list):
        raise ValueError(f"fields must be a list of field names.")

    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{data_path} does not contain a JSON array of objects.")

    new_data = []
    for obj in data:
        if isinstance(obj, dict):
            new_obj = {field: obj.get(field, None) for field in fields}
            new_data.append(new_obj)
        else:
            new_data.append(obj)

    with open(output_path, 'w', encoding='utf-8') as out_f:
        json.dump(new_data, out_f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(new_data)} objects with only specified fields to {output_path}")

def collect_field_values(input_file, output_file):
    """
    Reads a JSON array file, collects all unique values for each field across all objects,
    and writes the result as a dictionary to the output file.
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
    
    def safe_sort_key(x):
        if x is None:
            return (0, '')
        return (1, str(x))
    result = {k: sorted([json.loads(val) for val in vals], key=safe_sort_key) for k, vals in field_values.items()}
    with open(output_file, 'w', encoding='utf-8') as out_f:
        json.dump(result, out_f, indent=2, ensure_ascii=False)
    print(f"Extracted field values for {len(result)} fields to {output_file}")

def generate_normalization_map(input_path, fields_to_keep):
    if not os.path.isfile(input_path):
        print(f"File not found: {input_path}")
        sys.exit(1)
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    normalization_map = {}
    for field, values in data.items():
        if field in fields_to_keep:
            value_map = {}
            for value in values:
                # Handle strings directly, dump others to handle all value types as keys
                key = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
                value_map[key] = ""
            
            normalization_map[field] = {
                "value_mappings": value_map,
                "dynamic_rules": [],
                "default": None
            }
    return normalization_map

# =============================================================================
#  --- Dynamic Transformation Functions ---
# =============================================================================

def transform_to_uppercase(value):
    """Converts a string value to uppercase."""
    if isinstance(value, str):
        return value.upper()
    return value

# =============================================================================
#  --- Function Registry ---
# =============================================================================

FUNCTION_REGISTRY = {
    'to_uppercase': transform_to_uppercase,
}

def apply_dynamic_rule(value, rule):
    """
    Applies a dynamic rule to a value.
    """
    condition = rule.get('if', {})
    action = rule.get('then')

    for op, op_val in condition.items():
        if op == '$in' and value in op_val:
            return action
        elif op == '$regex':
            if isinstance(value, str) and re.search(op_val, value):
                return action
        elif op == 'apply_function':
            func = FUNCTION_REGISTRY.get(op_val)
            if func:
                return func(value)
    return None

def normalize_field_value(normalization_map, data_path, output_path):
    """
    Normalize field values in a JSON array using a normalization map.
    The map can contain direct value mappings and dynamic rules.
    """
    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    normalized_data = []
    for item in data:
        new_item = item.copy()
        for field, value in item.items():
            if field in normalization_map:
                field_config = normalization_map[field]
                
                # 1. Try direct value mapping first
                value_mappings = field_config.get('value_mappings', {})
                
                # Handle different types for lookup
                lookup_key = json.dumps(value, sort_keys=True)
                
                if lookup_key in value_mappings:
                    new_item[field] = value_mappings[lookup_key]
                    continue

                # 2. Apply dynamic rules
                rules = field_config.get('dynamic_rules', [])
                rule_applied = False
                for rule in rules:
                    result = apply_dynamic_rule(value, rule)
                    if result is not None:
                        new_item[field] = result
                        rule_applied = True
                        break
                
                if rule_applied:
                    continue

                # 3. Apply default value if no mapping or rule matched
                if 'default' in field_config:
                    new_item[field] = field_config['default']
                
        normalized_data.append(new_item)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(normalized_data, f, indent=2, ensure_ascii=False)

    print(f"Normalized {len(normalized_data)} items. Output written to {output_path}")

def generate_id(obj):
    obj_copy = dict(obj)
    obj_copy.pop('id', None)
    obj_str = json.dumps(obj_copy, sort_keys=True, separators=(",", ":"))
    hash_hex = hashlib.sha256(obj_str.encode("utf-8")).hexdigest()
    uuid_like = f"{hash_hex[:8]}-{hash_hex[8:12]}-{hash_hex[12:16]}-{hash_hex[16:20]}-{hash_hex[20:32]}-{hash_hex[32:64]}"
    return uuid_like

def add_ids_to_data(input_path, output_path):
    print(f"Reading input file: {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    count = 0
    if isinstance(data, list):
        for obj in data:
            obj["id"] = generate_id(obj)
            count += 1
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
    import argparse

    parser = argparse.ArgumentParser(description="Data normalization pipeline CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # --- Sub-parser for concatenate ---
    parser_concat = subparsers.add_parser("concatenate", help="Concatenate multiple JSON files.")
    parser_concat.add_argument("input_files", nargs="+", help="List of input JSON file paths.")
    parser_concat.add_argument("--output", required=True, help="Output file path.")

    # --- Sub-parser for collect_fields ---
    parser_collect = subparsers.add_parser("collect_fields", help="Collect all unique fields from JSON files.")
    parser_collect.add_argument("input_files", nargs="+", help="List of input JSON file paths.")
    parser_collect.add_argument("--output", required=True, help="Output file for the list of fields.")

    # --- Sub-parser for standardize_fields ---
    parser_standardize = subparsers.add_parser("standardize", help="Standardize objects in a data file based on a fields file.")
    parser_standardize.add_argument("--fields-file", required=True, help="Path to JSON file with the list of fields to keep.")
    parser_standardize.add_argument("--data-file", required=True, help="Path to the data file to standardize.")
    parser_standardize.add_argument("--output", required=True, help="Path for the output standardized data file.")

    # --- Sub-parser for collect_field_values ---
    parser_values = subparsers.add_parser("collect_values", help="Collect all unique values for each field.")
    parser_values.add_argument("--input-file", required=True, help="Path to the input data file.")
    parser_values.add_argument("--output", required=True, help="Path for the output file with field values.")

    # --- Sub-parser for generate_normalization_map ---
    parser_map = subparsers.add_parser("generate_map", help="Generate a blank normalization map.")
    parser_map.add_argument("--input-file", required=True, help="Path to the field values file.")
    parser_map.add_argument("--output", required=True, help="Path for the output normalization map.")

    # --- Sub-parser for normalize_field_value ---
    parser_normalize = subparsers.add_parser("normalize", help="Normalize data using a normalization map.")
    parser_normalize.add_argument("--map-file", required=True, help="Path to the normalization map.")
    parser_normalize.add_argument("--data-file", required=True, help="Path to the data file to normalize.")
    parser_normalize.add_argument("--output", required=True, help="Path for the output normalized data file.")

    # --- Sub-parser for add_ids ---
    parser_ids = subparsers.add_parser("add_ids", help="Add a unique 'id' to each object.")
    parser_ids.add_argument("--input-file", required=True, help="Path to the input data file.")
    parser_ids.add_argument("--output", required=True, help="Path for the output data file with IDs.")

    args = parser.parse_args()

    if args.command == "concatenate":
        concatenate_json_files(args.input_files, args.output)
    elif args.command == "collect_fields":
        collect_fields_from_json_files(args.input_files, args.output)
    elif args.command == "standardize":
        standardize_fields(args.fields_file, args.data_file, args.output)
    elif args.command == "collect_values":
        collect_field_values(args.input_file, args.output)
    elif args.command == "generate_map":
        generate_normalization_map(args.input_file, args.output)
    elif args.command == "normalize":
        normalize_field_value(args.map_file, args.data_file, args.output)
    elif args.command == "add_ids":
        add_ids_to_data(args.input_file, args.output)

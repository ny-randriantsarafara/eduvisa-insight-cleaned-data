import os
import shutil
import datetime
from dataprocessing.normalize import (
    concatenate_json_files,
    collect_fields_from_json_files,
    standardize_fields,
    collect_field_values,
    generate_normalization_map,
    normalize_field_value,
    add_ids_to_data,
)
from loading.load_to_db import load_to_mongodb

import json


def main():
    # Define file paths
    raw_data_dir = 'data/raw'
    interim_data_dir = 'data/intermediate'
    processed_data_dir = 'data/processed'
    pipeline_setup_dir = 'pipeline-setup'
    backup_dir = 'backups'

    config_file = os.path.join(pipeline_setup_dir, 'config.yml')
    config_file = os.path.join(pipeline_setup_dir, 'config.json')

    # Load config or create a new one if it doesn't exist
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
    else:
        print(
            f"Config file not found at {config_file}. A new one will be generated.")
        config = {
            "fields_to_keep": [],
            "normalization_map": {}
        }
    fields_to_keep = config['fields_to_keep']
    normalization_map = config['normalization_map']

    # Ensure directories exist
    os.makedirs(interim_data_dir, exist_ok=True)
    os.makedirs(processed_data_dir, exist_ok=True)
    os.makedirs(backup_dir, exist_ok=True)

    # Step 0: Concatenate raw data
    raw_files = [os.path.join(raw_data_dir, f) for f in os.listdir(
        raw_data_dir) if f.endswith('.json')]
    concatenated_file = os.path.join(interim_data_dir, 'concatenated.json')
    print("--- Step 0: Concatenating files ---")
    concatenate_json_files(raw_files, concatenated_file)

    # Step 1: Collect fields
    fields_file = os.path.join(interim_data_dir, 'all-fields.json')
    print("\n--- Step 1: Collecting fields ---")
    collect_fields_from_json_files([concatenated_file], fields_file)

    # --- Optional Step: Curate fields ---
    regenerate_fields = input(
        "Do you want to regenerate the 'fields_to_keep' list? (y/N): ").lower().strip() == 'y'
    if regenerate_fields:
        # Back up the existing config file before overwriting it
        if os.path.exists(config_file):
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            backup_file = os.path.join(
                backup_dir, f"config.json.{timestamp}.bak")
            print(f"Backing up existing config.json to {backup_file}")
            shutil.copy(config_file, backup_file)

        # Overwrite fields-to-keep in config.json with the latest from all-fields.json
        with open(fields_file, 'r') as f:
            all_fields = json.load(f)
        config['fields_to_keep'] = all_fields
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

        print(f"\n--- Manual Step: Edit the list of fields to keep ---")
        print(
            f"The 'fields_to_keep' section in 'config.json' has been regenerated at: {config_file}")
        print("Please review this file to curate the fields for the final dataset.")
        input("Press Enter to continue after editing the file...")

        # Reload config after manual edit
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
    else:
        print("Skipping 'fields_to_keep' regeneration.")

    # Step 2: Standardize fields
    standardized_file = os.path.join(interim_data_dir, 'standardized.json')
    print("\n--- Step 2: Standardizing fields ---")
    standardize_fields(fields_to_keep, concatenated_file, standardized_file)

    # Step 3: Extract field values
    field_values_file = os.path.join(interim_data_dir, 'field-values.json')
    print("\n--- Step 3: Extracting field values ---")
    collect_field_values(standardized_file, field_values_file)

    # Step 4: Generate normalization map
    print("\n--- Step 4: Generating normalization map ---")
    regenerate_map = input(
        "Do you want to regenerate the 'normalization_map'? (y/N): ").lower().strip() == 'y'
    if regenerate_map:
        # Backup existing config before overwriting
        if os.path.exists(config_file):
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            backup_file = os.path.join(
                backup_dir, f"config.json.{timestamp}.bak")
            print(f"Backing up existing config to {backup_file}")
            shutil.copy(config_file, backup_file)

        new_normalization_map = generate_normalization_map(field_values_file)
        config['normalization_map'] = new_normalization_map
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

        print(
            f"Please review and edit the normalization map in: {config_file}")
        input("Press Enter to continue after editing the map...")

        # Reload config after manual edit
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
    else:
        # Step 5: Normalize field values
        print("Skipping 'normalization_map' regeneration.")
    normalized_file = os.path.join(processed_data_dir, 'normalized-data.json')
    print("\n--- Step 5: Normalizing field values ---")
    normalize_field_value(
        normalization_map, standardized_file, normalized_file)

    # Step 6: Generate IDs
    final_data_file = os.path.join(
        processed_data_dir, 'normalized-data-with-ids.json')
    print("\n--- Step 6: Generating IDs ---")
    add_ids_to_data(normalized_file, final_data_file)

    # Step 7: Load to MongoDB
    load_to_db = input(
        "\nDo you want to load the data to MongoDB? (y/N): ").lower().strip() == 'y'
    if load_to_db:
        print("\n--- Step 7: Loading to MongoDB ---")
        mongo_uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DB_NAME")
        collection_name = os.getenv("MONGO_COLLECTION_NAME")
        load_to_mongodb(final_data_file, db_name, collection_name, mongo_uri)
    else:
        print("\nSkipping loading to MongoDB.")

    print("\n--- Pipeline finished ---")


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    main()

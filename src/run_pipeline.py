import os
import shutil
import datetime
from data_processing.normalize import (
    concatenate_json_files,
    collect_fields_from_json_files,
    standardize_fields,
    collect_field_values,
    generate_normalization_map,
    normalize_field_value,
    add_ids_to_data,
)
from loading.load_to_db import load_to_mongodb

def main():
    # Define file paths
    raw_data_dir = 'data/raw'
    interim_data_dir = 'data/interim'
    processed_data_dir = 'data/processed'

    # Ensure directories exist
    os.makedirs(interim_data_dir, exist_ok=True)
    os.makedirs(processed_data_dir, exist_ok=True)

    # Step 0: Concatenate raw data
    raw_files = [os.path.join(raw_data_dir, f) for f in os.listdir(raw_data_dir) if f.endswith('.json')]
    concatenated_file = os.path.join(interim_data_dir, 'concatenated.json')
    print("--- Step 0: Concatenating files ---")
    concatenate_json_files(raw_files, concatenated_file)

    # Step 1: Collect fields
    fields_file = os.path.join(interim_data_dir, 'all-fields.json')
    print("\n--- Step 1: Collecting fields ---")
    collect_fields_from_json_files([concatenated_file], fields_file)

    # --- Manual Step: Curate fields ---
    fields_to_keep_file = os.path.join(interim_data_dir, 'fields-to-keep.json')
    shutil.copy(fields_file, fields_to_keep_file)
    print(f"\n--- Manual Step: Edit the list of fields to keep ---")
    print(f"A file 'fields-to_keep.json' has been created at: {fields_to_keep_file}")
    print("Please edit this file to remove any fields you do not want to include in the final dataset.")
    input("Press Enter to continue after editing the file...")

    # Step 2: Standardize fields
    standardized_file = os.path.join(interim_data_dir, 'standardized.json')
    print("\n--- Step 2: Standardizing fields ---")
    standardize_fields(fields_to_keep_file, concatenated_file, standardized_file)

    # Step 3: Extract field values
    field_values_file = os.path.join(interim_data_dir, 'field-values.json')
    print("\n--- Step 3: Extracting field values ---")
    collect_field_values(standardized_file, field_values_file)

    # Step 4: Generate normalization map
    normalization_map_file = os.path.join(interim_data_dir, 'field-normalization-map.json')
    print("\n--- Step 4: Generating normalization map ---")
    
    # Backup existing map before overwriting
    if os.path.exists(normalization_map_file):
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        backup_file = f"{normalization_map_file}.{timestamp}.bak"
        print(f"Backing up existing normalization map to {backup_file}")
        shutil.copy(normalization_map_file, backup_file)

    generate_normalization_map(field_values_file, normalization_map_file)
    print(f"Please review and edit the normalization map at: {normalization_map_file}")
    input("Press Enter to continue after editing the map...")

    # Step 5: Normalize field values
    normalized_file = os.path.join(processed_data_dir, 'normalized-data.json')
    print("\n--- Step 5: Normalizing field values ---")
    normalize_field_value(normalization_map_file, standardized_file, normalized_file)

    # Step 6: Generate IDs
    final_data_file = os.path.join(processed_data_dir, 'normalized-data-with-ids.json')
    print("\n--- Step 6: Generating IDs ---")
    add_ids_to_data(normalized_file, final_data_file)

    # Step 7: Load to MongoDB
    print("\n--- Step 7: Loading to MongoDB ---")
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB_NAME")
    collection_name = os.getenv("MONGO_COLLECTION_NAME")
    load_to_mongodb(final_data_file, db_name, collection_name, mongo_uri)

    print("\n--- Pipeline finished ---")

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    main()

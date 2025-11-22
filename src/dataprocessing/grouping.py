import json
import os
from typing import Any, Dict, List

def group_fields(normalized_data: List[Dict[str, Any]], grouping_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Groups fields in the normalized data based on the provided grouping configuration.

    Args:
        normalized_data (List[Dict[str, Any]]): The list of normalized data records.
        grouping_config (Dict[str, Any]): The configuration for grouping fields.

    Returns:
        List[Dict[str, Any]]: The list of data records with fields grouped.
    """
    grouped_data = []
    for record in normalized_data:
        grouped_record = _process_grouping_level(record, grouping_config)
        grouped_data.append(grouped_record)
    return grouped_data

def _process_grouping_level(record: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively processes one level of the grouping configuration.

    Args:
        record (Dict[str, Any]): A single data record.
        config (Dict[str, Any]): The grouping configuration for the current level.

    Returns:
        Dict[str, Any]: The part of the record with fields grouped for this level.
    """
    grouped_part = {}
    if 'id' in config and config['id'] == 'id':
        if 'id' in record:
            grouped_part['id'] = record['id']

    for key, value in config.items():
        if key == 'id':
            continue
        if isinstance(value, str):
            if value in record:
                grouped_part[key] = record[value]
        elif isinstance(value, dict):
            grouped_part[key] = _process_grouping_level(record, value)
    return grouped_part

def run_grouping(config: Dict[str, Any]):
    """
    Runs the data grouping process.

    Args:
        config (Dict[str, Any]): The pipeline configuration.
    """
    normalized_data_path = config["normalizedDataPath"]
    grouping_config_path = config["groupingConfigPath"]
    grouped_data_path = config["groupedDataPath"]

    print("Starting data grouping...")

    if not os.path.exists(normalized_data_path):
        print(f"Error: Normalized data file not found at {normalized_data_path}")
        return

    if not os.path.exists(grouping_config_path):
        print(f"Error: Grouping config file not found at {grouping_config_path}")
        return

    with open(normalized_data_path, 'r', encoding='utf-8') as f:
        normalized_data = json.load(f)

    with open(grouping_config_path, 'r', encoding='utf-8') as f:
        grouping_config = json.load(f)

    grouped_data = group_fields(normalized_data, grouping_config)

    output_dir = os.path.dirname(grouped_data_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(grouped_data_path, 'w', encoding='utf-8') as f:
        json.dump(grouped_data, f, indent=2, ensure_ascii=False)

    print(f"Data grouping complete. Grouped data saved to {grouped_data_path}")

if __name__ == '__main__':
    # This part is for testing the script directly
    # You would typically run it from run_pipeline.py
    with open('pipeline-setup/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    run_grouping(config['dataPaths'])

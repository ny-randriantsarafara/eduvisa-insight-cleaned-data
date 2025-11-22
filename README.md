# Eduvisa Insight

This project provides a data processing pipeline for cleaning, normalizing, and preparing educational visa application data for analysis. The pipeline is designed to handle raw JSON data from multiple sources, standardize its structure, and apply normalization rules to ensure data quality and consistency.

## Project Structure

-   `data/`: Contains raw, intermediate, and processed data.
    -   `raw/`: Original, untouched JSON data files.
    -   `intermediate/`: Temporary files generated during the pipeline.
    -   `processed/`: Final, cleaned, and normalized data.
-   `notebooks/`: Jupyter notebooks for data exploration and analysis (if any).
-   `pipeline-setup/`: Configuration files for the data processing pipeline.
    -   `config.json`: Main configuration file for the pipeline, including paths and settings.
-   `src/`: Source code for the data processing pipeline.
    -   `dataprocessing/normalize.py`: Core script containing all pipeline functions.
    -   `run_pipeline.py`: Script to execute the full data processing pipeline based on `config.json`.
-   `requirements.txt`: A list of Python dependencies required for this project.

## Data Processing Pipeline

The pipeline consists of several steps, orchestrated by `src/dataprocessing/normalize.py`. Each step can be run individually via the command line.

### Pipeline Steps

1.  **Concatenate**: Combines multiple raw JSON files into a single file.
2.  **Collect Fields**: Scans the concatenated data to identify all unique field names.
3.  **Standardize**: Ensures every data object has the same set of fields, adding `null` for missing ones and removing extraneous ones.
4.  **Collect Values**: Gathers all unique values for each field to help in creating normalization rules.
5.  **Generate Map**: Creates a template `normalization-map.json` file where you can define rules for cleaning and standardizing values.
6.  **Normalize**: Applies the rules from the normalization map to the data. This is where data cleaning happens.
7.  **Add IDs**: Generates a unique, content-based ID for each data record.

## Dynamic Rules for Data Normalization

The dynamic rules feature allows you to transform field values using conditional logic. This is useful for cleaning up data, standardizing formats, or deriving new values based on patterns. These rules are defined in your `config.json` file (or any file you use as a normalization map) within the `dynamic_rules` array for each field.

### Structure

Each field in the normalization map can have a `dynamic_rules` array. Each element in the array is an object with an `if` condition and a `then` action. The rules are evaluated in the order they appear, and the first matching rule's `then` value is applied.

```json
{
  "your_field_name": {
    "value_mappings": {
      "some_value": "a specific new value"
    },
    "dynamic_rules": [
      {
        "if": { "operator": "condition_value" },
        "then": "new_value_if_condition_met"
      }
    ],
    "default": "a_default_value"
  }
}
```

### Supported `if` Conditions

You can use the following operators in the `if` block:

1.  **`$in`**: Checks if the field's value is present in a given list.

    -   **Example**: If the value is one of `["USA", "U.S.A", "United States"]`, normalize it to `"United States of America"`.

        ```json
        {
          "if": { "$in": ["USA", "U.S.A", "United States"] },
          "then": "United States of America"
        }
        ```

2.  **`$regex`**: Matches the field's value against a regular expression. This is only for string values.

    -   **Example**: If the value contains a year like "2023", "2024", etc., classify it as `"Recent"`.

        ```json
        {
          "if": { "$regex": "202[3-9]" },
          "then": "Recent"
        }
        ```

3.  **`apply_function`**: Applies a pre-defined function to the value. The result of the function will be the new value.

    -   **Example**: Convert the field's value to uppercase.

        ```json
        {
          "if": { "apply_function": "to_uppercase" },
          "then": null 
        }
        ```
        *Note: When using `apply_function`, the `then` value is ignored, and the function's return value is used instead. The function is applied to the value, and the result of the function is the new value.*

### Available Functions

The following functions are available in the `FUNCTION_REGISTRY` in `src/dataprocessing/normalize.py`:

-   `to_uppercase`: Converts a string to uppercase.

You can extend this by adding more functions to the `FUNCTION_REGISTRY` dictionary in `src/dataprocessing/normalize.py`.

### Example: `normalization_map.json`

Here is an example for a field named `"country"`:

```json
{
  "country": {
    "value_mappings": {
      "UK": "United Kingdom"
    },
    "dynamic_rules": [
      {
        "if": { "$in": ["USA", "U.S.A", "United States"] },
        "then": "United States of America"
      },
      {
        "if": { "apply_function": "to_uppercase" },
        "then": null
      }
    ],
    "default": "Other"
  }
}
```

In this example, for the `country` field:
1.  A direct mapping from `"UK"` to `"United Kingdom"` is tried first.
2.  If that doesn't match, it checks if the value is in `["USA", "U.S.A", "United States"]` and changes it to `"United States of America"`.
3.  If still no match, it applies the `to_uppercase` function to the value.
4.  If none of the above rules apply, the value will be set to the `"default"` value, which is `"Other"`.

## Usage

To run the pipeline, you can execute the steps individually using the CLI commands available in `src/dataprocessing/normalize.py`.

### Example Commands

```bash
# Concatenate raw data files
python src/dataprocessing/normalize.py concatenate data/raw/2023.json data/raw/2024.json --output data/intermediate/concatenated.json

# Collect all unique fields
python src/dataprocessing/normalize.py collect_fields data/intermediate/concatenated.json --output data/intermediate/all-fields.json

# Normalize data using a map
python src/dataprocessing/normalize.py normalize --map-file pipeline-setup/normalization-map.json --data-file data/intermediate/standardized.json --output data/processed/normalized-data.json

# Add unique IDs
python src/dataprocessing/normalize.py add_ids --input-file data/processed/normalized-data.json --output data/processed/normalized-data-with-ids.json
```

Alternatively, you can run the entire pipeline by executing `src/run_pipeline.py`.

```bash
python src/run_pipeline.py
```

## Dependencies

Install the required Python packages using `pip`:

```bash
pip install -r requirements.txt
```

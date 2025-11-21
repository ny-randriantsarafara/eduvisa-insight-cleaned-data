import json
import os
from pymongo import MongoClient
import argparse
from dotenv import load_dotenv

# Ensure you have pymongo and python-dotenv installed:
# pip install pymongo python-dotenv

def load_to_mongodb(file_path, db_name, collection_name, mongo_uri):
    """
    Loads data from a JSON file into a MongoDB collection.

    :param file_path: Path to the JSON file.
    :param db_name: Name of the MongoDB database.
    :param collection_name: Name of the MongoDB collection.
    :param mongo_uri: MongoDB connection string.
    """
    if not all([mongo_uri, db_name, collection_name]):
        print("Error: MONGO_URI, MONGO_DB_NAME, and MONGO_COLLECTION_NAME environment variables must be set.")
        return

    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Check if the file exists
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return

        # Read the JSON file
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Insert or upsert data into the collection
        if isinstance(data, list):
            if data:
                upserted_count = 0
                for doc in data:
                    # Use 'id' as the unique key for upsert
                    filter_query = {"id": doc["id"]} if "id" in doc else doc
                    result = collection.replace_one(filter_query, doc, upsert=True)
                    if result.upserted_id:
                        print(f"Upserted document with id: {doc.get('id')}")
                        upserted_count += 1
                    else:
                        print(f"Updated existing document with id: {doc.get('id')}")
                print(f"Successfully upserted {upserted_count} new documents into '{collection_name}'.")
            else:
                print("JSON file is empty. No data upserted.")
        else:
            filter_query = {"id": data["id"]} if "id" in data else data
            result = collection.replace_one(filter_query, data, upsert=True)
            if result.upserted_id:
                print(f"Upserted document with id: {data.get('id')}")
            else:
                print(f"Updated existing document with id: {data.get('id')}")
            print(f"Successfully upserted 1 document into '{collection_name}'.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env file

    parser = argparse.ArgumentParser(description="Load a JSON file into a MongoDB collection.")
    parser.add_argument("file_path", help="The path to the JSON file.")
    
    args = parser.parse_args()

    # Get MongoDB connection details from environment variables
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB_NAME")
    collection_name = os.getenv("MONGO_COLLECTION_NAME")

    load_to_mongodb(args.file_path, db_name, collection_name, mongo_uri)

    # Example usage from the command line:
    # 1. Create a .env file with your credentials (see .env.example)
    # 2. Run the script:
    # python loaders/load-to-mongodb.py normalized-data.json

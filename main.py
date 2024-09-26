import os
import bson
import pymongo


client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
db = client["hellobible_preprod"] 

# Path to the dump database
bson_directory = "hellobible_preprod"

# check all file on the path where the dump are stored
for filename in os.listdir(bson_directory):
    if filename.endswith(".bson"):
        collection_name = filename.replace(".bson", "")
        collection = db[collection_name] 
        
        bson_file_path = os.path.join(bson_directory, filename)
        print(f"Importing {bson_file_path} into collection {collection_name}...")

        with open(bson_file_path, "rb") as f:
            try:
                data = bson.decode_all(f.read())  
                if data:
                    collection.insert_many(data)  # insert the data
                    print(f"Inserted {len(data)} documents into {collection_name}.")
                else:
                    print(f"No documents found in {bson_file_path}.")
            except Exception as e:
                print(f"Error occurred while importing {bson_file_path}: {e}")

# Fermer la connexion MongoDB
client.close()

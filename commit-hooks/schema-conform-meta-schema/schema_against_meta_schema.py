#!/usr/bin/python3

import sys
import json
import argparse
import jsonschema
from fill_refs_schema import fill_refs


def validate_schema(schema_file_name, schema, schema_folder_path):
    meta_data_schema_uri = schema['$schema']
    if 'http' in meta_data_schema_uri:
        # Find schema via its URL locally
        meta_data_file_name = meta_data_schema_uri.replace("://", "_")
    elif 'tag' in meta_data_schema_uri:
        # Find schema via its tag locally
        meta_data_file_name = meta_data_schema_uri.replace("tag:", "tag_")
        meta_data_file_name = meta_data_schema_uri.replace(":", "_")
    else:
        print('Cannot validate schema because no meta_data schema is' +
              ' found in the folder where the schema can be found.')
        sys.exit(1)
    # Check if the path to the schema exists in the schemas folder
    meta_data_file_name = meta_data_file_name.replace("/", "_")
    if not schema_folder_path.endswith("/"):
        schema_folder_path = schema_folder_path + "/"
    meta_data_schema_path = schema_folder_path + meta_data_file_name + ".json"
    try:
        with open(meta_data_schema_path, 'r') as f:
            meta_data_schema = json.load(f)
    except Exception:
        schema_id = schema.get('$id')
        if schema_id:
            print("Could not validate schema agains meta schema because could not find meta schema {}".format(meta_data_file_name) +
                  " , it should be in the same folder as the schema {}".format(schema_id))
        else:
            print("The schema {} does not have an ID field".format(schema_file_name))
        sys.exit(1)
    try:
        # Also fill in references if they occur in the meta data schema
        meta_data_schema = fill_refs(meta_data_schema, schema_folder_path)
    except Exception as e:
        print("Could not fill in references in the meta_data_schema because of {}".format(e))
        sys.exit(1)
    # Validate the schema agains the meta data schema
    try:
        jsonschema.validate(schema, meta_data_schema)
    except Exception as e:
        print("Schema is not conform meta data schema" +
              " because of {}".format(e))
        sys.exit(1)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-jf', '--json-file', required=True)
    args = parser.parse_args()
    file_name = args.json_file
    # Get file name of json
    json_file_name = file_name.split("/")[-1]
    # Open json that needs to be verified
    try:
        with open(args.json_file, 'r') as f:
            json_file = json.load(f)
    except Exception as e:
        print("Exception occured when trying to open json, reason {}".format(e))
        sys.exit(1)
    if '$schema' in json_file:
        # Get folder where json file is from
        original_folder = file_name.replace(json_file_name, '')
        if original_folder:
            # fill in references in schema
            try:
                json_file = fill_refs(json_file, original_folder)
            except Exception as e:
                print("Could not fill in references in the schema because of {}".format(e))
                sys.exit(1)
            if validate_schema(json_file_name, json_file, original_folder) is False:
                sys.exit(1)
        else:
            print("Folder of meta schema cannot be found")
            sys.exit(1)

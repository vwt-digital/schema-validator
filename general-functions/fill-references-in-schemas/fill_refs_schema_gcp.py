#!/usr/bin/python3

from functools import reduce
import operator
from io import StringIO
import json
import sys
from google.cloud import storage
import os

storage_client_external = storage.Client()
schemas_bucket = os.environ.get('SCHEMAS_BUCKET_NAME', 'Required parameter is missing')
storage_bucket = storage_client_external.get_bucket(schemas_bucket)


def fill_refs(schema):
    schema_json = schema
    new_schema = StringIO()
    schema = json.dumps(schema, indent=2)
    # Make schema into list so that every newline can be printed
    schema_list = schema.split('\n')
    for line in schema_list:
        if '$ref' in line:
            # If a '#' is in the reference, it's a reference to a definition
            if '#' in line:
                if '"$ref": "' in line:
                    line_array_def = line.split('"$ref": "')
                elif '"$ref" : "' in line:
                    line_array_def = line.split('"$ref" : "')
                else:
                    line_array_def = ''
                ref_def = line_array_def[1].replace('\"', '')
                # If the reference is only '#'
                if ref_def == '#':
                    # Just write it to the stringio file
                    new_schema.write(line)
                else:
                    # Check if there is a URI in front of the '#'
                    # Because then the definition is in another schema
                    comma_at_end = False
                    if 'tag' in ref_def or 'http' in ref_def:
                        # Split on the '#/'
                        ref_def_array = ref_def.split("#/")
                        uri_part = ref_def_array[0]
                        def_part = ref_def_array[1]
                        if def_part[-1] == ',':
                            def_part = def_part[:-1]
                            comma_at_end = True
                        # Get file from GCP storage
                        reference_schema_def = get_schema_from_stg(uri_part)
                    else:
                        # Reference to definition is in this schema
                        # Split on the '#/'
                        ref_def_array = ref_def.split("#/")
                        def_part = ref_def_array[1]
                        # If there's a comma at the end
                        if def_part[-1] == ',':
                            def_part = def_part[:-1]
                            comma_at_end = True
                        reference_schema_def = schema_json
                    # Now find key in json where the reference is defined
                    def_part_array = def_part.split('/')
                    def_from_dict = getFromDict(reference_schema_def, def_part_array)
                    # If type of definition reference is dict
                    if type(def_from_dict) is dict:
                        # put reference in stringio file
                        def_from_dict_txt = json.dumps(def_from_dict, indent=2)
                        def_from_dict_list = def_from_dict_txt.split('\n')
                        for i in range(len(def_from_dict_list)):
                            # Do not add the beginning '{' and '}'
                            if i != 0 and i != (len(def_from_dict_list)-1):
                                line_to_write = def_from_dict_list[i]
                                # Write the reference schema to the stringio file
                                if i == len(def_from_dict_list)-2 and comma_at_end:
                                    line_to_write = "{},".format(line_to_write)
                                new_schema.write(line_to_write)
                    else:
                        print("Definition should be dict")
            # If reference is a URL
            elif 'http' in line:
                if '"$ref": "' in line:
                    line_array = line.split('"$ref": "')
                elif '"$ref" : "' in line:
                    line_array = line.split('"$ref" : "')
                else:
                    line_array = ''
                ref = line_array[1].replace('\"', '')
                # Fill reference
                new_schema = fill_from_local_schema(new_schema, ref)
            # If reference is a tag
            elif 'tag' in line:
                if '"$ref": "' in line:
                    line_array = line.split('"$ref": "')
                elif '"$ref" : "' in line:
                    line_array = line.split('"$ref" : "')
                else:
                    line_array = ''
                ref = line_array[1].replace('\"', '')
                # Fill reference
                new_schema = fill_from_local_schema(new_schema, ref)
            else:
                # If the line does not contain any tag references
                # Just write it to the stringio file
                new_schema.write(line)
        else:
            # If the line does not contain any references
            # Just write it to the stringio file
            new_schema.write(line)
    contents = new_schema.getvalue()
    new_schema = json.loads(contents)
    return new_schema


def fill_from_local_schema(new_schema, meta_data_uri):
    # Get schema from GCP
    reference_schema = get_schema_from_stg(meta_data_uri)
    try:
        # Also fill in references if they occur in the reference schema
        reference_schema = fill_refs(reference_schema)
    except Exception as e:
        if '$id' in reference_schema:
            print("The references in schema {} ".format(reference_schema['$id']) +
                  "could not be filled because of {}".format(e))
        else:
            print("The references in a schema " +
                  "could not be filled because of {}".format(e))
        sys.exit(1)
    else:
        # Double check if the uri of the schema is the same as the
        # one of the reference
        if '$id' in reference_schema:
            if reference_schema['$id'] == meta_data_uri:
                # Add the schema to the new schema
                reference_schema_txt = json.dumps(reference_schema, indent=2)
                reference_schema_list = reference_schema_txt.split('\n')
                for i in range(len(reference_schema_list)):
                    # Do not add the beginning '{' and '}'
                    if i != 0 and i != (len(reference_schema_list)-1):
                        # Write the reference schema to the stringio file
                        new_schema.write(reference_schema_list[i])
            else:
                print("ID of reference is {} while ".format(meta_data_uri) +
                      "that of the schema is {}".format(reference_schema['$id']))
                sys.exit(1)
        else:
            print("Reference schema of reference {} has no ID".format(meta_data_uri))
            sys.exit(1)
    return new_schema


def get_schema_from_stg(tag):
    # Get schemas bucket from other project
    blob_name = schema_name_from_tag(tag)
    # Check if schema is in schema storage
    if storage.Blob(bucket=storage_bucket, name=blob_name).exists(storage_client_external):
        # Get blob
        blob = storage_bucket.get_blob(blob_name)
        # Convert to string
        blob_json_string = blob.download_as_string()
        # Convert to json
        blob_json = json.loads(blob_json_string)
        # return blob in json format
        return blob_json
    return None


def schema_name_from_tag(tag):
    tag = tag.replace('/', '_')
    if not tag.endswith(".json"):
        tag = tag + ".json"
    return tag


# This function traverses the dictionary and gets the value of a key from a list of attributes
def getFromDict(dataDict, mapList):
    return reduce(operator.getitem, mapList, dataDict)

import config
import os
import logging
import sys
from google.cloud import storage
import json


class SchemaProcessor(object):

    def __init__(self):
        self.meta = config.SCHEMA_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        self.bucket_name = os.environ.get('BUCKET_NAME', 'Required parameter is missing')

    def process(self, payload):
        schemas = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]

        for schema in schemas:
            return_bool_upload_blob = self.upload_to_storage(schema, self.bucket_name)
            if not return_bool_upload_blob:
                sys.exit(1)

    def upload_to_storage(self, schema, bucket_name):
        schema_id = schema['$id']
        if not schema_id.endswith(".json"):
            schema_id = schema_id + ".json"
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)
            # Find out if schema is already in bucket
            blobs = storage_client.list_blobs(bucket_name)
            blobs_to_delete = []
            for blob in blobs:
                # If blob is already in bucket
                if blob.name == self.schema_name_from_tag(schema_id):
                    # Remove it because it could be an older version of the schema
                    blobs_to_delete.append(blob.name)
            for blob_name in blobs_to_delete:
                logging.info('Schema {} is already in storage, deleting'.format(blob_name))
                blob = bucket.blob(blob_name)
                blob.delete()
            # Now add the schema to the storage
            blob = bucket.blob(self.schema_name_from_tag(schema_id))
            blob.upload_from_string(
                data=json.dumps(schema),
                content_type='application/json'
            )
            logging.info('Uploaded schema {} to bucket {}'.format(schema_id, bucket_name))
            return True
        except Exception as e:
            logging.exception('Unable to upload schema ' +
                              'to storage because of {}'.format(e))
        return False

    def schema_name_from_tag(self, schema_name):
        schema_name = schema_name.replace('/', '_')
        return schema_name

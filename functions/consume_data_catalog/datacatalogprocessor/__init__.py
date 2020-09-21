import config
import os
import logging
import sys
from google.cloud import storage
import json


class DatacatalogProcessor(object):

    def __init__(self):
        self.meta = config.DATACATALOG_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        self.bucket_name = os.environ.get('BUCKET_NAME', 'Required parameter is missing')

    def process(self, payload):
        selector_data = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]

        # Upload data catalog to storage
        return_bool_upload_blob = self.upload_to_storage(selector_data, self.bucket_name)
        if not return_bool_upload_blob:
            sys.exit(1)

    def upload_to_storage(self, data_catalog, bucket_name):
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)
            # Find out if data catalog is already in bucket
            blobs = storage_client.list_blobs(bucket_name)
            blobs_to_delete = []
            data_catalog_id = data_catalog['projectId']
            for blob in blobs:
                # If blob is already in bucket
                if blob.name == data_catalog_id:
                    # Remove it because it could be an older version of the data catalog
                    blobs_to_delete.append(blob.name)
            for blob_name in blobs_to_delete:
                logging.info('Data catalog {} is already in storage, deleting'.format(blob_name))
                blob = bucket.blob(blob_name)
                blob.delete()
            # Now add the data catalog to the storage
            blob = bucket.blob(data_catalog_id)
            blob.upload_from_string(
                data=json.dumps(data_catalog),
                content_type='application/json'
            )
            logging.info('Uploaded data catalog {} to bucket {}'.format(data_catalog_id, bucket_name))
            return True
        except Exception as e:
            logging.exception('Unable to upload data catalog ' +
                              'to storage because of {}'.format(e))
        return False

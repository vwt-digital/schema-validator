import logging
import datetime
import json
from io import BytesIO
import gzip
import jsonschema
import os

from google.cloud import storage, pubsub_v1


class MessageValidator(object):
    def __init__(self):
        self.storage_client = storage.Client()
        self.publisher = pubsub_v1.PublisherClient()
        self.schemas_bucket_name = os.environ.get('SCHEMAS_BUCKET_NAME', 'Required parameter is missing')
        self.data_catalogs_bucket_name = os.environ.get('DATA_CATALOGS_BUCKET_NAME', 'Required parameter is missing')

    def validate(self):
        # For every data catalog in the data catalog bucket
        for blob in self.storage_client.list_blobs(
                    self.data_catalogs_bucket_name):
            blob_to_string = blob.download_as_string()
            blob_to_json = json.loads(blob_to_string)
            # Validate the messages that every topic with a schema has
            self.check_messages(blob_to_json)

    def check_messages(self, catalog):
        topics_with_schema = []
        # Check in data catalog what topic has a schema
        for dataset in catalog['dataset']:
            for dist in dataset.get('distribution', []):
                if dist.get('format') == 'topic':
                    # Get dataset topic only if it has a schema
                    if 'describedBy' in dist and 'describedByType' in dist:
                        # Get schema urn and topic title
                        schema_urn = dist.get('describedBy')
                        topic_name = dist.get('title', 'unknown')
                        # Put them in JSON
                        topic_schema_info = {
                            "schema_urn": schema_urn,
                            "topic_name": topic_name
                        }
                        # Add info to list
                        topics_with_schema.append(topic_schema_info)

        storage_client = storage.Client()
        # For every topic with a schema
        for ts in topics_with_schema:
            logging.info("The messages of topic {} are validated against schema {}".format(
                ts['topic_name'], ts['schema_urn']))
            # There is a history storage bucket
            ts_history_bucket_name = ts['topic_name'] + "-history-stg"
            schema_bucket = storage_client.get_bucket(self.schemas_bucket_name)
            # Get schema from schema bucket belonging to this topic
            schema_urn_simple = ts['schema_urn'].replace('/', '-')
            schema = schema_bucket.get_blob(schema_urn_simple)
            schema = json.loads(schema.download_as_string())
            # Want to check the messages of the previous day
            yesterday = datetime.date.today()-datetime.timedelta(1)
            month = '{:02d}'.format(yesterday.month)
            day = '{:02d}'.format(yesterday.day)
            bucket_folder = '{}/{}/{}'.format(yesterday.year, month, day)
            # For every blob in this bucket
            for blob in self.storage_client.list_blobs(
                        ts_history_bucket_name, prefix=bucket_folder):
                zipbytes = BytesIO(blob.download_as_string())
                with gzip.open(zipbytes, 'rb') as gzfile:
                    filecontent = gzfile.read()
                messages = json.loads(filecontent)
                for msg in messages:
                    try:
                        jsonschema.validate(msg, schema)
                    except Exception as e:
                        logging.exception('Message is not conform schema' +
                                          ' because of {}'.format(e))
            logging.info("All messages are conform schema")


def validate_messages(request):
    logging.info("Initialized function")

    MessageValidator().validate()


if __name__ == '__main__':
    validate_messages(None)

import logging
import datetime
import json
import config
from io import BytesIO
import gzip
import jsonschema

from google.cloud import storage, pubsub_v1


class MessageValidator(object):
    def __init__(self):
        self.storage_client = storage.Client()
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = config.PROJECT_ID
        self.project_path = self.publisher.project_path(self.project_id)
        self.schema_bucket_name = config.SCHEMA_BUCKET_NAME

    def check_messages(self, catalog):
        topics_with_schema = []
        # Check in data catalog if a topic has a schema
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
                            "topic_name": topic_name,
                            "topic_urn": self.project_path + "/topics/" + topic_name
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
            schema_bucket = storage_client.get_bucket(self.schema_bucket_name)
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

    try:
        with open('/home/bente/VWT/ODH/operational-data-hub-config/config/vwt-d-gew1-odh-hub/data_catalog.json', 'r') as f:
            catalog = json.load(f)

        MessageValidator().check_messages(catalog)

    except Exception as e:
        logging.exception('Unable to compare schema ' +
                          'because of {}'.format(e))


if __name__ == '__main__':
    validate_messages(None)

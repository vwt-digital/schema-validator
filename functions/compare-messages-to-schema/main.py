import logging
import datetime
import json
from io import BytesIO
import gzip
import jsonschema
import os
import config

from google.cloud import storage, pubsub_v1
import google.auth
from google.auth.transport import requests as gcp_requests
from google.auth import iam
from google.oauth2 import service_account

TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'  # nosec


class MessageValidator(object):
    def __init__(self):
        self.storage_client = storage.Client()
        self.publisher = pubsub_v1.PublisherClient()
        self.schemas_bucket_name = os.environ.get('SCHEMAS_BUCKET_NAME', 'Required parameter is missing')
        self.data_catalogs_bucket_name = os.environ.get('DATA_CATALOGS_BUCKET_NAME', 'Required parameter is missing')
        self.external_credentials = request_auth_token()
        self.storage_client_external = storage.Client(credentials=self.external_credentials)

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

        # For every topic with a schema
        for ts in topics_with_schema:
            logging.info("The messages of topic {} are validated against schema {}".format(
                ts['topic_name'], ts['schema_urn']))
            # There is a history storage bucket
            ts_history_bucket_name = ts['topic_name'] + "-history-stg"
            schema_bucket = self.storage_client.get_bucket(self.schemas_bucket_name)
            # Get schema from schema bucket belonging to this topic
            schema_urn_simple = ts['schema_urn'].replace('/', '-')
            schema = schema_bucket.get_blob(schema_urn_simple)
            schema = json.loads(schema.download_as_string())
            # Want to check the messages of the previous day
            yesterday = datetime.date.today()-datetime.timedelta(1)
            month = '{:02d}'.format(yesterday.month)
            day = '{:02d}'.format(yesterday.day)
            bucket_folder = '{}/{}/{}'.format(yesterday.year, month, day)
            blob_exists = False
            # For every blob in this bucket
            for blob in self.storage_client_external.list_blobs(
                        ts_history_bucket_name, prefix=bucket_folder):
                blob_exists = True
                zipbytes = BytesIO(blob.download_as_string())
                with gzip.open(zipbytes, 'rb') as gzfile:
                    filecontent = gzfile.read()
                # Get its messages
                messages = json.loads(filecontent)
                for msg in messages:
                    try:
                        # Validate every message against the schema of the topic
                        # of the bucket
                        jsonschema.validate(msg, schema)
                    except Exception as e:
                        logging.exception('Message is not conform schema' +
                                          ' because of {}'.format(e))
            if blob_exists is False:
                logging.info("No new messages were published yesterday")
            else:
                logging.info("All messages are conform schema")


def request_auth_token():
    try:
        credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])

        request = gcp_requests.Request()
        credentials.refresh(request)

        signer = iam.Signer(request, credentials, config.DELEGATED_SA)
        creds = service_account.Credentials(
            signer=signer,
            service_account_email=config.DELEGATED_SA,
            token_uri=TOKEN_URI,
            scopes=['https://www.googleapis.com/auth/cloud-platform'],
            subject=config.DELEGATED_SA)
    except Exception:
        raise

    return creds


def validate_messages(request):
    logging.info("Initialized function")

    MessageValidator().validate()


if __name__ == '__main__':
    validate_messages(None)

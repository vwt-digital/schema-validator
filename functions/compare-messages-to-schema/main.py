import logging
import datetime
import json
from io import BytesIO
import gzip
import jsonschema
import os
import config
import atlassian
import secretmanager
import time

from google.cloud import storage, pubsub_v1
import google.auth
from google.auth.transport import requests as gcp_requests
from google.auth import iam
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)

TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'  # nosec


class MessageValidator(object):
    def __init__(self):
        self.storage_client = storage.Client()
        self.publisher = pubsub_v1.PublisherClient()
        self.schemas_bucket_name = os.environ.get('SCHEMAS_BUCKET_NAME', 'Required parameter is missing')
        self.data_catalogs_bucket_name = os.environ.get('DATA_CATALOGS_BUCKET_NAME', 'Required parameter is missing')
        self.external_credentials = request_auth_token()
        self.storage_client_external = storage.Client(credentials=self.external_credentials)
        self.project_id = os.environ.get('PROJECT_ID', 'Required parameter is missing')
        self.timeout = os.environ.get('TIMEOUT', 'Required parameter is missing')
        self.validate_time_per_topic = 0

    def validate(self):
        total_messages_not_conform_schema = []
        # For every data catalog in the data catalog bucket
        for blob in self.storage_client.list_blobs(
                    self.data_catalogs_bucket_name):
            blob_to_string = blob.download_as_string()
            blob_to_json = json.loads(blob_to_string)
            # Validate the messages that every topic with a schema has
            messages_not_conform_schema = self.check_messages(blob_to_json)
            total_messages_not_conform_schema.extend(messages_not_conform_schema)
        if len(total_messages_not_conform_schema) > 0:
            self.create_jira_tickets(total_messages_not_conform_schema)

    def check_messages(self, catalog):
        messages_not_conform_schema = []
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

        # Set time to check per topic
        # It's the total time the function can take minus half a minute divided by the total number of topics
        if len(topics_with_schema) > 0:
            self.validate_time_per_topic = (int(self.timeout) - 30)/len(topics_with_schema)
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
            year = yesterday.year
            month = '{:02d}'.format(yesterday.month)
            day = '{:02d}'.format(yesterday.day)
            bucket_folder = '{}/{}/{}'.format(year, month, day)
            blob_exists = False
            start_time = time.time()
            # For every blob in this bucket
            for blob in self.storage_client_external.list_blobs(
                        ts_history_bucket_name, prefix=bucket_folder):
                blob_exists = True
                blob_full_name = blob.name
                zipbytes = BytesIO(blob.download_as_string())
                with gzip.open(zipbytes, 'rb') as gzfile:
                    filecontent = gzfile.read()
                # Get its messages
                messages = json.loads(filecontent)
                for msg in messages:
                    try:
                        # Check if the time is already over the max time
                        if time.time() - start_time >= self.validate_time_per_topic:
                            break
                        # Validate every message against the schema of the topic
                        # of the bucket
                        jsonschema.validate(msg, schema)
                    except Exception as e:
                        logging.info('Message is not conform schema' +
                                     ' because of {}'.format(e))
                        msg_info = {
                            "schema_urn": ts['schema_urn'],
                            "topic_name": ts['topic_name'],
                            "history_bucket": ts_history_bucket_name,
                            "blob_full_name": blob_full_name
                        }
                        if msg_info not in messages_not_conform_schema:
                            messages_not_conform_schema.append(msg_info)
                # Check if the time is already over the max time
                if time.time() - start_time >= self.validate_time_per_topic:
                    break
            if blob_exists is False:
                logging.info("No new messages were published yesterday")
            elif time.time() - start_time >= self.validate_time_per_topic:
                logging.info("Too many messages uploaded yesterday, did not check all. "
                             "The ones that were checked are conform schema.")
            else:
                logging.info("All messages are conform schema")
        return messages_not_conform_schema

    def create_jira_tickets(self, messages_not_conform_schema):
        # Jira config
        jira_user = config.JIRA_USER
        jira_server = config.JIRA_SERVER
        jira_project = config.JIRA_PROJECT
        jira_board = config.JIRA_BOARD
        jira_epic = config.JIRA_EPIC
        jira_api_key = secretmanager.get_secret(
            self.project_id,
            config.JIRA_SECRET_ID)

        client = atlassian.jira_init(jira_user, jira_api_key, jira_server)

        # Get current sprint
        sprint_id = atlassian.get_current_sprint(client, jira_board)

        logging.info(f"Possibly creating tickets for sprint {sprint_id} of project {jira_project}...")

        # For every message that is not conform the schema of its topic
        for msg_info in messages_not_conform_schema:
            # Jira jql to find tickets that already exist conform these issues
            jql = f"project = {jira_project} " \
                "AND type = Bug AND status != Done AND status != Cancelled " \
                f"AND \"Epic Link\" = {jira_epic} " \
                "AND text ~ \"Message not conform schema\" " \
                "ORDER BY priority DESC".format(msg_info['blob_full_name'])
            # Get issues that are already conform the 'issue template'
            titles = atlassian.list_issue_titles(client, jql)
            # Make issue
            title = "Message not conform schema: topic '{}' schema '{}'".format(
                msg_info['topic_name'], msg_info['schema_urn'])
            description = "The topic `{}` got a message in blob {} that is not conform its schema ({}). " \
                          "Please check why the message is not conform the schema. " \
                          "The message can be found in history bucket {}. " \
                          "Other folders in this bucket might also contain wrong messages".format(
                              msg_info['topic_name'], msg_info['blob_full_name'],
                              msg_info['schema_urn'], msg_info['history_bucket'])
            # Check if Jira ticket already exists for this topic with this schema
            if title not in titles:
                logging.info(f"Creating jira ticket: {title}")
                # Create a Jira ticket
                issue = atlassian.create_issue(
                    client=client,
                    project=jira_project,
                    title=title,
                    description=description)
                # Add Jira ticket to sprint and epic
                atlassian.add_to_sprint(client, sprint_id, issue.key)
                atlassian.add_to_epic(client, jira_epic, issue.key)


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

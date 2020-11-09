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
        self.timeout = int(os.environ.get('TIMEOUT', 540))
        self.validate_time_per_topic = 0
        self.max_response_size = int(os.environ.get('MAX_RESPONSE_SIZE', 10))
        self.response_size = 0
        self.response_size_per_topic = 0

    def validate(self):
        total_messages_not_conform_schema = []
        # For every data catalog in the data catalog bucket
        for blob in self.storage_client.list_blobs(
                    self.data_catalogs_bucket_name):
            self.response_size = self.response_size + (blob.size / 1000000)
            blob_to_string = blob.download_as_string()
            blob_to_json = json.loads(blob_to_string)
            # Validate the messages that every topic with a schema has
            messages_not_conform_schema = self.check_messages(blob_to_json)
            total_messages_not_conform_schema.extend(messages_not_conform_schema)
        if len(total_messages_not_conform_schema) > 0:
            try:
                self.create_jira_tickets(total_messages_not_conform_schema)
            except Exception as e:
                logging.error(f"Could not create JIRA tickets due to {e}")

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
        # Also set max response mb per topic for the blobs
        if len(topics_with_schema) > 0:
            self.validate_time_per_topic = (self.timeout - 30)/len(topics_with_schema)
            response_size_now = self.response_size
            self.response_size_per_topic = (self.max_response_size - response_size_now) / len(topics_with_schema)
            self.response_size_per_topic = self.response_size_per_topic
            self.response_size = 0
        topics_checked = 0
        # For every topic with a schema
        for ts in topics_with_schema:
            topic_checked = False
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
            msgs_not_conform_schema = False
            blobs_checked = 0
            for blob in self.storage_client_external.list_blobs(
                        ts_history_bucket_name, prefix=bucket_folder):
                blobs_checked = blobs_checked + 1
                blob_exists = True
                blob_full_name = blob.name
                blob_size = blob.size / 1000000
                self.response_size = self.response_size + blob_size
                try:
                    # Check if response size is already over max response size
                    # And at least one blob has already been checked
                    if self.response_size >= self.response_size_per_topic and blobs_checked > 1:
                        logging.info("Max response size for this topic is reached")
                        self.response_size = 0
                        break
                    zipbytes = BytesIO(blob.download_as_string())
                except Exception as e:
                    logging.error(f"Could not download blob as string due to {e}")
                try:
                    with gzip.open(zipbytes, 'rb') as gzfile:
                        filecontent = gzfile.read()
                except Exception as e:
                    logging.error(f"Could not unzip blob because of {e}")
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
                        if topic_checked is False:
                            topics_checked = topics_checked + 1
                            topic_checked = True
                    except jsonschema.exceptions.ValidationError as e:
                        msgs_not_conform_schema = True
                        msg_info = {
                            "schema_urn": ts['schema_urn'],
                            "topic_name": ts['topic_name'],
                            "history_bucket": ts_history_bucket_name,
                            "blob_full_name": blob_full_name,
                            "error": e
                        }
                        if msg_info not in messages_not_conform_schema:
                            messages_not_conform_schema.append(msg_info)
                # Check if the time is already over the max time
                # Or response size is already over max response size
                if time.time() - start_time >= self.validate_time_per_topic or \
                   self.response_size >= self.response_size_per_topic:
                    logging.info("Too many messages uploaded yesterday, did not check all. "
                                 "The ones that were checked are conform schema.")
                    self.response_size = 0
                    break
            if blob_exists is False:
                logging.info("No new messages were published yesterday")
                topics_checked = topics_checked + 1
            elif msgs_not_conform_schema:
                logging.info('Topic contains messages that are not conform schema')
            else:
                logging.info("Messages are conform schema")
        # Check if for all topics at least one message has been validated
        if topics_checked is not len(topics_with_schema):
            logging.error("At least one message per topic should be validated, check if validate_time_per_topic"
                          " and/or response_size_per_topic are high enough")
        return messages_not_conform_schema

    def create_jira_tickets(self, messages_not_conform_schema):
        # Jira config
        jira_user = config.JIRA_USER
        jira_server = config.JIRA_SERVER
        jira_project = config.JIRA_PROJECT
        jira_projects = config.JIRA_PROJECTS
        jira_board = config.JIRA_BOARD
        jira_epic = config.JIRA_EPIC
        jira_api_key = secretmanager.get_secret(
            self.project_id,
            config.JIRA_SECRET_ID)

        client = atlassian.jira_init(jira_user, jira_api_key, jira_server)

        # Get current sprint
        sprint_id = atlassian.get_current_sprint(client, jira_board)

        jira_projects_list = jira_projects.split('+')
        logging.info(f"Possibly creating or updating tickets for sprint {sprint_id} of projects {jira_projects_list}...")

        # Jira jql to find tickets that already exist conform these issues
        jql_prefix = f"type = Bug AND status != Done AND status != Cancelled " \
            f"AND \"Epic Link\" = {jira_epic} " \
            "AND text ~ \"Message not conform schema\" " \
            "AND project = "
        projects = [jql_prefix + project for project in jira_projects_list]
        jql = " OR ".join(projects)
        jql = f"{jql} ORDER BY priority DESC "

        made_comments = []
        # For every message that is not conform the schema of its topic
        for msg_info in messages_not_conform_schema:
            # Make issue
            title = "Messages not conform schema: topic '{}' schema '{}'".format(
                msg_info['topic_name'], msg_info['schema_urn'])
            # Error information
            e = msg_info['error']
            error_message = e.message
            error_absolute_schema_path = f"{list(e.absolute_schema_path)}"
            error_absolute_path = f"{list(e.absolute_path)}"
            error_value = f"{e.validator} '{e.validator_value}'"
            instance = f"{e.instance}"
            error = error_message.replace(f"{instance} ", "")
            # Check if the error was already commented in this session
            comment_info = {
                "error_absolute_path": error_absolute_path,
                "error_absolute_schema_path": error_absolute_schema_path,
                "error_value": error_value,
                "title": title
            }
            # If it is, skip the message
            if comment_info in made_comments:
                continue

            # Make comment
            comment_place = f"Wrong message can be found in blob {msg_info['blob_full_name']}" + \
                            f" in history bucket {msg_info['history_bucket']}"
            comment_error_msg_key = f"\nThe error in the message can be found in key: {error_absolute_path}"
            comment_error = f"\nThe error for this key is: {error}"
            comment_schema_key = f"\nIn the schema, the error can be found in key: {error_absolute_schema_path}"
            comment = comment_place + comment_error_msg_key + comment_error + comment_schema_key
            # Get issues that are already conform the 'issue template'
            titles = atlassian.list_issue_titles(client, jql)
            # Get issues with title
            issues = atlassian.list_issues(client, jql)
            # For every issue with this title
            for issue in issues:
                # Get comments of issues
                issue_id = atlassian.get_issue_id(client, issue)
                issue_comment_ids = atlassian.list_issue_comment_ids(client, issue_id)
                comment_bodies = []
                for comment_id in issue_comment_ids:
                    comment_body = atlassian.get_comment_body(client, issue, comment_id)
                    comment_bodies.append(comment_body)
            # Check if Jira ticket already exists for this topic with this schema
            if title not in titles:
                description = f"The topic `{msg_info['topic_name']}` received messages" + \
                              f" that are not conform its schema ({msg_info['schema_urn']})." + \
                              " The messages with their errors can be found in the comments of this ticket" + \
                              " Please check why the messages are not conform the schema. "
                logging.info(f"Creating jira ticket: {title}")
                # Create a Jira ticket
                issue = atlassian.create_issue(
                    client=client,
                    project=jira_project,
                    title=title,
                    description=description)
                # Add comment to jira ticket
                atlassian.add_comment(client, issue, comment)
                # Add Jira ticket to sprint and epic
                atlassian.add_to_sprint(client, sprint_id, issue.key)
                atlassian.add_to_epic(client, jira_epic, issue.key)
            # If it does exist, add a comment with the message and its error
            else:
                # Check if the comment does not yet exist
                if comment not in comment_bodies:
                    # Check if the error message has not already been created in this session
                    made_comments.append(comment_info)
                    # Add comment to jira ticket
                    atlassian.add_comment(client, issue_id, comment)


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

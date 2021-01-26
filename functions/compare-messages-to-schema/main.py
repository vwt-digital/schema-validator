import os
import sys
import logging

import time
import uuid
import json
import tarfile
import zlib
import jsonschema

import tickets
import auth

from datetime import datetime, timedelta
from google.cloud import storage
from fill_refs_schema import fill_refs

logging.basicConfig(level=logging.INFO)


class MessageValidator(object):
    def __init__(self, topic_name, messages_bucket_name, schema, schema_tag, max_process_time, process_start_time):
        """
        Initializes a class for validating messages
        """

        self.topic_name = topic_name
        self.messages_bucket_name = messages_bucket_name
        self.schema = schema
        self.schema_tag = schema_tag

        self.max_process_time = max_process_time
        self.process_start_time = process_start_time

    def validate(self, blob):
        """
        Validates messages from a blob against a schema
        """

        ok_status, messages = self.get_blob_messages(blob)
        if not ok_status:
            return [messages]

        messages_not_conform_schema = []

        for msg in messages:
            try:
                jsonschema.validate(msg, self.schema)
            except (jsonschema.exceptions.ValidationError, jsonschema.exceptions.SchemaError) as e:
                msg_info = {
                    "schema_tag": self.schema_tag,
                    "topic_name": self.topic_name,
                    "history_bucket": self.messages_bucket_name,
                    "blob_full_name": blob.name,
                    "type": "schema" if isinstance(e, jsonschema.exceptions.SchemaError) else "message",
                    "error": e
                }
                if msg_info not in messages_not_conform_schema:
                    messages_not_conform_schema.append(msg_info)
            finally:
                if time.time() - self.process_start_time >= self.max_process_time:
                    break

        return messages_not_conform_schema

    def get_blob_messages(self, blob):
        """
        Retrieves the messages from the blob file
        """

        messages = []

        try:
            if blob.content_type == 'application/json':
                messages.extend(json.loads(blob.download_as_string()))
            elif blob.content_type == 'application/x-xz':
                temp_directory = "/tmp"  # nosec
                temp_file_name = f"{temp_directory}/{str(uuid.uuid4())}.tar.xz"

                if not os.path.exists(temp_directory):
                    os.makedirs(temp_directory)

                blob.download_to_filename(temp_file_name)
                with tarfile.open(temp_file_name, mode='r:xz') as tar:
                    for member in tar.getmembers():
                        if member.name.endswith('.json'):
                            f = tar.extractfile(member)
                            messages.extend(json.loads(f.read()))

                os.remove(temp_file_name)
            elif blob.name.endswith('.archive.gz'):
                messages = json.loads(zlib.decompress(blob.download_as_string(), 16 + zlib.MAX_WBITS))
            else:
                logging.info(f"File format '{blob.content_type}' is not supported by the function")
                return False, None
        except Exception as e:
            logging.error(f"Could not unzip blob because of {str(e)}")
            message_not_conform_schema = {
                "schema_tag": self.schema_tag,
                "topic_name": self.topic_name,
                "history_bucket": self.messages_bucket_name,
                "blob_full_name": blob.name,
                "type": "blob",
                "error": f"Could not unzip blob because of {str(e)}"
            }
            return False, [message_not_conform_schema]
        else:
            return True, messages


class TopicProcessor(object):
    def __init__(self, stg_client, stg_client_ext, schemas_bucket_name, max_process_time, total_topics):
        """
        Initializes a class for processing topic data
        """

        self.stg_client = stg_client
        self.stg_client_ext = stg_client_ext

        self.max_process_time = max_process_time
        self.total_topics = total_topics

        self.schemas_bucket_name = schemas_bucket_name

        yesterday = datetime.now() - timedelta(1)
        self.bucket_prefix = datetime.strftime(yesterday, '%Y/%m/%d')

    def validate_topic_messages(self, topic_schema):
        """
        Validates a topic schema with help from Pub/Sub backup messages
        """

        process_start_time = time.time()

        topic_name = topic_schema['topic_name']
        topic_schema_tag = topic_schema['schema_tag']
        topic_messages_bucket_name = f"{topic_name}-history-stg"

        topic_schema = self.retrieve_topic_schema(topic_schema_tag)  # Retrieve the topic schema
        if not topic_schema:
            logging.info(f"No valid schema found for topic '{topic_name}'")
            return False, None

        topic_blobs = list(self.stg_client_ext.list_blobs(topic_messages_bucket_name, prefix=self.bucket_prefix))

        if len(topic_blobs) == 0:
            logging.info(f"No new messages of topic '{topic_name}' were published yesterday")
            self.update_max_process_time(process_start_time)
            return True, []
        else:
            logging.info(
                f"The messages of topic '{topic_name}' are validated against schema '{topic_schema_tag}'")
            topic_invalid_messages = []

            message_validator = MessageValidator(
                topic_name=topic_name, messages_bucket_name=topic_messages_bucket_name, schema=topic_schema,
                schema_tag=topic_schema_tag, max_process_time=self.max_process_time,
                process_start_time=process_start_time)
            for blob in topic_blobs:
                invalid_messages = message_validator.validate(blob)
                topic_invalid_messages.extend(invalid_messages)

            self.update_max_process_time(process_start_time)
            return True, topic_invalid_messages

    def retrieve_topic_schema(self, topic_schema_tag):
        """
        Retrieves and parses the topic schema from a schemas bucket
        """

        schema_tag_simple = topic_schema_tag.replace('/', '_')

        try:
            schemas_bucket = self.stg_client.get_bucket(self.schemas_bucket_name)
            schema_blob = schemas_bucket.get_blob(schema_tag_simple)
            schema = json.loads(schema_blob.download_as_string())
        except Exception as e:
            logging.error(f"Could not download schema '{schema_tag_simple}' due to {e}")
            pass
        else:
            return fill_refs(schema)  # Fill references within the schema

    def update_max_process_time(self, start_time):
        """
        Updates left maximum process time for future topics
        """

        process_time_left = self.max_process_time - (time.time() - start_time)
        self.max_process_time = self.max_process_time + (process_time_left / self.total_topics)


def retrieve_topics_schema(bucket):
    """
    Retrieves the info of all data-catalogs topics with a valid schema
    """

    catalog_topics = []

    for blob in bucket.list_blobs():
        catalog = json.loads(blob.download_as_string())

        for dataset in catalog['dataset']:
            for dist in dataset.get('distribution', []):
                if dist.get('format') == 'topic':
                    if 'describedBy' in dist and 'describedByType' in dist:
                        catalog_topics.append({
                            "schema_tag": dist['describedBy'],
                            "topic_name": dist.get('title', 'unknown')
                        })

    return catalog_topics


def validate_messages(request):
    logging.info("Initialized function")

    try:
        catalogs_bucket_name = os.environ.get('DATA_CATALOGS_BUCKET_NAME')
        schemas_bucket_name = os.environ.get('SCHEMAS_BUCKET_NAME')
        timeout = int(os.environ.get('TIMEOUT', 540))
    except KeyError as e:
        logging.error(f"Function is missing required environment variable: {str(e)}")
        sys.exit(1)
    else:
        stg_client = storage.Client()
        stg_client_ext = storage.Client(credentials=auth.request_auth_token())

        topic_schemas = retrieve_topics_schema(bucket=stg_client.get_bucket(catalogs_bucket_name))
        if len(topic_schemas) == 0:
            logging.info("No topics to process")
        else:
            invalid_messages = []
            validation_time_per_topic = (timeout - 30) / len(topic_schemas)

            topic_processor = TopicProcessor(
                stg_client=stg_client, stg_client_ext=stg_client_ext, schemas_bucket_name=schemas_bucket_name,
                max_process_time=validation_time_per_topic, total_topics=len(topic_schemas))

            for topic_schema in topic_schemas:
                ok_status, topic_invalid_messages = topic_processor.validate_topic_messages(
                    topic_schema)  # Validate messages for topic

                if ok_status:
                    invalid_messages.extend(topic_invalid_messages)

            if len(invalid_messages) > 0:
                try:
                    tickets.create_jira_tickets(invalid_messages)
                except Exception as e:
                    logging.error(f"Could not create JIRA tickets due to {e}")


if __name__ == '__main__':
    validate_messages(None)

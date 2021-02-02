import os
import io
import logging

import time
import re
import json
import lzma
import jsonschema

import google.auth.transport.requests as tr_requests

import tickets
import auth

from datetime import datetime, timedelta
from google.cloud import storage
from google.resumable_media.requests import ChunkedDownload
from fill_refs_schema import fill_refs

logging.basicConfig(level=logging.INFO)
logging.getLogger("google.resumable_media._helpers").setLevel(level=logging.ERROR)


class MessageValidator(object):
    def __init__(self, credentials_ext, topic_name, messages_bucket_name, schema, schema_tag, max_process_time, process_start_time):
        """
        Initializes a class for validating messages
        """

        self.credentials = credentials_ext

        self.topic_name = topic_name
        self.messages_bucket_name = messages_bucket_name
        self.schema = schema
        self.schema_tag = schema_tag

        self.max_process_time = max_process_time
        self.process_start_time = process_start_time

        self.transport = tr_requests.AuthorizedSession(self.credentials)

    def validate(self, blob):
        """
        Validates messages from a blob against a schema
        """

        if blob.content_type != 'application/x-xz':
            return

        messages_not_conform_schema = []

        try:
            media_url = blob.generate_signed_url(
                expiration=timedelta(seconds=self.max_process_time), method="GET", version='v4')
            chunk_size = 512000  # 500KB
            stream = io.BytesIO()
            count = 0

            download = ChunkedDownload(media_url, chunk_size, stream)
            lzd = lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=52428800)

            last_json = None

            while not download.finished:
                response = download.consume_next_chunk(self.transport)
                decoded_data = lzd.decompress(response.content).decode('utf-8')

                if count == 0:
                    decoded_data = re.sub(r'^.*?\[\{', '[{', decoded_data)

                if last_json:
                    decoded_data = last_json + decoded_data

                new_messages_not_conform_schema, last_json = self.validate_dirty_json(decoded_data, blob.name)
                messages_not_conform_schema.extend(new_messages_not_conform_schema)
                count += 1

                if time.time() - self.process_start_time >= self.max_process_time:
                    break
        except Exception as e:
            logging.error(f"Could not unzip blob because of {str(e)}")
            messages_not_conform_schema.append({
                "schema_tag": self.schema_tag,
                "topic_name": self.topic_name,
                "history_bucket": self.messages_bucket_name,
                "blob_full_name": blob.name,
                "type": "blob",
                "error": f"Could not unzip blob because of {str(e)}"
            })

        return messages_not_conform_schema

    def validate_dirty_json(self, dirty_json, blob_name):
        messages_not_conform_schema = []

        parsed_json, last_json = self.parse_dirty_json(dirty_json)

        for msg in parsed_json:
            try:
                jsonschema.validate(msg, self.schema)
            except (jsonschema.exceptions.ValidationError, jsonschema.exceptions.SchemaError) as e:
                msg_info = {
                    "schema_tag": self.schema_tag,
                    "topic_name": self.topic_name,
                    "history_bucket": self.messages_bucket_name,
                    "blob_full_name": blob_name,
                    "type": "schema" if isinstance(e, jsonschema.exceptions.SchemaError) else "message",
                    "error": str(e)
                }
                if msg_info not in messages_not_conform_schema:
                    messages_not_conform_schema.append(msg_info)
            finally:
                if time.time() - self.process_start_time >= self.max_process_time:
                    break

        return messages_not_conform_schema, last_json

    def parse_dirty_json(self, dirty_json):
        """
        Parse a dirty string towards a list of JSON objects
        """

        bracket_strings, timed_out = self.divide_string(dirty_json)  # Divide string into json chunks

        parsed_json = []
        unparsed_json = []
        for string in bracket_strings:
            try:
                json_data = json.loads(string)
            except json.decoder.JSONDecodeError:
                unparsed_json.append(string)
                break
            else:
                parsed_json.append(json_data)
            finally:
                if time.time() - self.process_start_time >= self.max_process_time:
                    timed_out = True
                    break

        last_json = None if timed_out or len(unparsed_json) == 0 else ''.join(unparsed_json)
        return parsed_json, last_json

    def divide_string(self, string):
        """
        Divide string based on { ... } format
        """

        timed_out = False
        current_bracket_count = 0
        current_bracket_string = ''

        bracket_strings = []
        for i, v in enumerate(string):
            if v == "{":
                current_bracket_count += 1
            if v == "}":
                current_bracket_count -= 1

            if current_bracket_count > 0:
                current_bracket_string = current_bracket_string + v

            if current_bracket_count == 0 and len(current_bracket_string) > 0:
                bracket_strings.append(current_bracket_string + v)
                current_bracket_string = ''

            if time.time() - self.process_start_time >= self.max_process_time:
                timed_out = True
                break

        if len(current_bracket_string) > 0 and not timed_out:
            bracket_strings.append(current_bracket_string)

        return bracket_strings, timed_out


class TopicProcessor(object):
    def __init__(self, stg_client, stg_client_ext, credentials_ext, schemas_bucket_name, max_process_time, total_topics):
        """
        Initializes a class for processing topic data
        """

        self.stg_client = stg_client
        self.stg_client_ext = stg_client_ext
        self.credentials_ext = credentials_ext

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
                credentials_ext=self.credentials_ext, topic_name=topic_name, messages_bucket_name=topic_messages_bucket_name,
                schema=topic_schema, schema_tag=topic_schema_tag, max_process_time=self.max_process_time,
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

        if self.total_topics > 1:
            self.total_topics = self.total_topics - 1

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
        return 'Bad Request', 400
    else:
        credentials_ext, project_id = auth.request_auth_token()

        stg_client = storage.Client()
        stg_client_ext = storage.Client(credentials=credentials_ext)

        topic_schemas = retrieve_topics_schema(bucket=stg_client.get_bucket(catalogs_bucket_name))
        if len(topic_schemas) == 0:
            logging.info("No topics to process")
        else:
            invalid_messages = []
            validation_time_per_topic = (timeout - 30) / len(topic_schemas)

            topic_processor = TopicProcessor(
                stg_client=stg_client, stg_client_ext=stg_client_ext, credentials_ext=credentials_ext,
                schemas_bucket_name=schemas_bucket_name, max_process_time=validation_time_per_topic,
                total_topics=len(topic_schemas))

            for topic_schema in topic_schemas:
                ok_status, topic_invalid_messages = topic_processor.validate_topic_messages(topic_schema)  # Validate messages for topic

                if ok_status:
                    invalid_messages.extend(topic_invalid_messages)

            if len(invalid_messages) > 0:
                try:
                    tickets.create_jira_tickets(invalid_messages, project_id)
                except Exception as e:
                    logging.error(f"Could not create JIRA tickets due to {e}")
                    return 'Bad Request', 400

    return 'OK', 204


if __name__ == '__main__':
    validate_messages(None)

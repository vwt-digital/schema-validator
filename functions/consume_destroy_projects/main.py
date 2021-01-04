import logging
import json
import base64
import os
from google.cloud import storage


logging.basicConfig(level=logging.INFO)


def process_destroy_projects(msg):
    bucket_name = os.environ.get('BUCKET_NAME')

    if not bucket_name:
        logging.error('Required environment variable BUCKET_NAME is missing')
        return

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    if msg.get('destroy_projects'):
        for proj in msg['destroy_projects']:
            project_id = proj['project_id']
            logging.debug(f'Checking existence of data catalog for {project_id}')
            blob = bucket.blob(project_id)
            if blob.exists():
                logging.info(f'Deleting data catalog of {project_id}')
                blob.delete()
            else:
                logging.debug(f'Data catalog for {project_id} was already deleted')
    else:
        logging.info('No projects specified to destroy in received message')


def handler(request):
    # Extract data from request
    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    # Extract subscription from subscription string
    try:
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Message received from {subscription} [{payload}]')

        process_destroy_projects(json.loads(payload))

    except Exception as e:
        logging.info('Extract of subscription failed')
        logging.debug(e)
        raise e

    # Returning any 2xx status indicates successful receipt of the message.
    # 204: no content, delivery successfull, no further actions needed
    return 'OK', 204


if __name__ == '__main__':
    logging.info('Testing consume_destroy_projects')
    msg = {
        'destroy_projects': [
            {
                'project_id': 'vwt-d-gew1-dat-hallo'
            }
        ]
    }

    process_destroy_projects(msg)

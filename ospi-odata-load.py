#!python3

"""
Script for slurping all data available in the Washington State OSPI service
endpoint and stuffing it into bigquery.
"""

import argparse
import json
import logging
import random
import requests
import xml.etree.ElementTree as ET

from fastavro import writer, parse_schema
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY
from odata import get_schemas, get_entity_sets


logger = logging.getLogger(__name__)
storage_client = storage.Client(project='sps-btn-data')
bucket = storage_client.bucket('sps-btn-data-all-data')


ODATA_ENDPOINT = 'https://data.wa.gov/api/odata/v4'


def getMetadata():
    response = requests.get(ODATA_ENDPOINT + '/$metadata')
    if response.status_code != 200:
        raise ValueError(response)

    return response.text


def to_avro_schema(schema, name):
    fields = []
    for field_name, type_info in schema.items():
        field_info = {}
        field_info['name'] = field_name
        for k, v in type_info['avro_type'].items():
            field_info[k] = v
        fields.append(field_info)
    avro_schema = {
        'name': name.replace('-', '_'),
        'type': 'record',
        'fields': fields,
    }
    return avro_schema


def entity_path(name):
    return f'raw/ospi/odata/{name}'


def checkpoint_exists(name):
    return bucket.blob(f'{entity_path(name)}.done').exists()


def write_checkpoint(name, value):
    logger.info(f'CHECKPOINT {name}: writing: {repr(value)}')
    return bucket.blob(f'{entity_path(name)}.done').upload_from_string(
        json.dumps(value),
        content_type="application/json",
        retry=DEFAULT_RETRY)


def bq_load_all_entities(schemas, entity_sets, tempfile):
    random.shuffle(entity_sets)
    for entity in entity_sets:
        if checkpoint_exists(entity):
            logger.info(f'CHECKPOINT {entity}: skip')
            continue

        logger.info(f'Processing {entity}')
        avro_schema = to_avro_schema(schemas[entity], entity)

        next_url = f'{ODATA_ENDPOINT}/{entity}'
        try:
            opened_file = None
            while next_url is not None:
                response = requests.get(next_url)
                if response.status_code != 200:
                    # No access to data. Skip!
                    write_checkpoint(entity,
                                     {'ok': False,
                                      'status_code': response.status_code,
                                      'msg': response.text})
                    break

                data = json.loads(response.text)
                next_url = data.get('@odata.nextLink', None)

                if opened_file is None:
                    opened_file = open(tempfile, 'wb+')
                    writer(opened_file, parse_schema(avro_schema),
                           data['value'], codec='zstandard')
                else:
                    writer(opened_file, None, data['value'], codec='zstandard')

            if opened_file is not None:
                opened_file.close()
                opened_file = None
                bucket.blob(
                    f'{entity_path(entity)}.avro').upload_from_filename(
                        tempfile, content_type="application/avro",
                        retry=DEFAULT_RETRY)

                write_checkpoint(entity, {'ok': True})
            else:
                write_checkpoint(entity, {'ok': False, 'message': 'No data?'})
        finally:
            if opened_file:
                opened_file.close()


def main():
    parser = argparse.ArgumentParser(
        description='Snags data from ospi')
    parser.add_argument('--metadata', type=argparse.FileType('r'),
                        help=('If set, use XML file for metadata instead of '
                              'getting it from the server.'))
    parser.add_argument('--log-level', default='INFO',
                        help='set log level {DEBUG, INFO, WARNING, ERROR}')
    parser.add_argument('--tmpfile', required=True,
                        help='tmpfile for the avro')
    parser.add_argument('--entity-set', default=None,
                        help='Do just one entity set')

    args = parser.parse_args()
    logging.basicConfig(level=args.log_level)
    if args.metadata:
        metadata = ET.parse(args.metadata)
    else:
        metadata = ET.fromstring(getMetadata())

    schemas = get_schemas(metadata)
    if args.entity_set:
        entity_sets = [args.entity_set]
    else:
        entity_sets = get_entity_sets(metadata)

    bq_load_all_entities(schemas, entity_sets, args.tempfile)


if __name__ == "__main__":
    main()

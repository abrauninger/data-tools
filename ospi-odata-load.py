#!python3

"""
Script for slurping all data available in the Washington State OSPI service
endpoint and stuffing it into bigquery.
"""

import argparse
import requests
import functools
import xml.etree.ElementTree as ET
from odata import edm_to_js_type

ODATA_ENDPOINT = 'https://data.wa.gov/api/odata/v4'
ODATA_NS = {'edmx': 'http://docs.oasis-open.org/odata/ns/edmx',
            'edm': 'http://docs.oasis-open.org/odata/ns/edm'}

SOCRATE_PREFIX = 'socrata.'


def merge_property(metadata, schema, prop_name, prop_type):
    """Converts the property into a SQL schema.

    May add multiple properties if there are complex ones.
    """
    if prop_type.startswith('Edm.'):
        # Simple type. All good.
        schema[prop_name] = edm_to_js_type(prop_type)
    elif prop_type.startswith(SOCRATE_PREFIX):
        sub_schema = complex_type_to_schema(metadata,
                                            prop_type[len(SOCRATE_PREFIX):])
        for subprop_name, subprop_value in sub_schema.items():
            schema[f'{prop_name}_{subprop_name}'] = subprop_value


@functools.cache
def complex_type_to_schema(metadata, type_name):
    schema = {}
    type_element = metadata.findall(f'.//edm:ComplexType[@Name="{type_name}"]',
                                    ODATA_NS)[0]
    for prop in type_element.findall('./edm:Property', ODATA_NS):
        prop_type = prop.get('Type')
        if prop_type.startswith('Edm.'):
            schema[prop.get('Name')] = edm_to_js_type(prop_type)
        elif prop_type.startswith('socrata.'):
            subschema = complex_type_to_schema(metadata, prop_type[8:])
            for subprop_name, subprop_value in subschema:
                schema[f'{type_name}_{subprop_name}'] = subprop_value
    return schema


def get_schemas(metadata):
    schemas = {}
    for entity_type in metadata.findall('.//edm:EntityType', ODATA_NS):
        cur_schema = {}
        for prop in entity_type.findall('./edm:Property', ODATA_NS):
            merge_property(metadata, cur_schema, prop.get('Name'),
                           prop.get('Type'))

        fourfour_id = entity_type.get('Name')
        schemas[fourfour_id] = cur_schema

    return schemas


def getMetadata():
    response = requests.get(ODATA_ENDPOINT + '/$metadata')
    if response.status_code != 200:
        raise ValueError(response)

    return response.text


def main():
    parser = argparse.ArgumentParser(
        description='Snags data from ospi')
    parser.add_argument('--metadata', type=argparse.FileType('r'),
                        help=('If set, use XML file for metadata instead of '
                              'getting it from the server.'))

    args = parser.parse_args()
    if args.metadata:
        metadata = ET.parse(args.metadata)
    else:
        metadata = ET.fromstring(getMetadata())

    schemas = get_schemas(metadata)
    entity_set_elements = metadata.findall('.//edm:Service/edm:EntitySet',
                                           ODATA_NS)
    print(schemas, entity_set_elements)


if __name__ == "__main__":
    main()

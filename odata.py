import functools
from datetime import datetime, timedelta, timezone

ODATA_NS = {'edmx': 'http://docs.oasis-open.org/odata/ns/edmx',
            'edm': 'http://docs.oasis-open.org/odata/ns/edm'}

SOCRATA_PREFIX = 'socrata.'
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def identity(x):
    return x


def _since_epoch(x):
    parsed_date = datetime.fromisoformat(x)
    return (parsed_date - EPOCH)


def transform_edm_date_to_epoch_days(x):
    if x is None:
        return x

    return _since_epoch(x).days


def transform_edm_date_to_millis(x):
    if x is None:
        return x

    return round(_since_epoch(x) / timedelta(milliseconds=1))


def transform_edm_point_to_pointliteral(x):
    if x is None:
        return x

    if x['type'] != 'Point':
        raise ValueError(x)
    coordinates = x['coordinates']
    return f'POINT({coordinates[0]} {coordinates[1]})'


def transform_edm_multiline_to_multilineliteral(x):
    if x is None:
        return x

    if x['type'] != 'MultiLineString':
        raise ValueError(x)
    coordinates = x['coordinates']
    # Array of Lines.
    # Each line is an array of points.
    return 'MULTILINESTRING (%s)' % (','.join(
        ['(%s)' % ','.join([ f'{point[0]} {point[1]}' for point in line ])
         for line in coordinates]))


def transform_edm_linestring_to_linestring(x):
    if x is None:
        return x

    if x['type'] != 'LineString':
        raise ValueError(x)
    coordinates = x['coordinates']
    # Array of Lines.
    # Each line is an array of points.
    return 'LINESTRING (%s)' % ','.join([
        f'{point[0]} {point[1]}' for point in coordinates])


def transform_edm_multipoint_to_multipoint(x):
    if x is None:
        return x

    if x['type'] != 'MultiPoint':
        raise ValueError(x)
    coordinates = x['coordinates']
    # Array of Lines.
    # Each line is an array of points.
    return 'MULTIPOINT (%s)' % ','.join([
        f'{point[0]} {point[1]}' for point in coordinates])


def transform_edm_multipolygon_to_multipolygon(x):
    if x is None:
        return x

    if x['type'] != 'MultiPolygon':
        raise ValueError(x)

    coordinates = x['coordinates']
    # Array of Polygons
    # Polygon is array of mulitpoints
    # A multipoint is an array of points
    return 'MULTIPOLYGON (%s)' % ','.join(
        ['(%s)' % ','.join(  # One polygon
                           ['(%s)' % ','.join( # One multipoint
                                              [f'{point[0]} {point[1]}'
                                               for point in multipoint])
                            for multipoint in polygon])
         for polygon in coordinates])


def edm_to_schema_type(edm_node):
    edm_type = edm_node.get('Type')
    match edm_type:
        case 'Edm.Binary':
            return {'sql_type': 'BYTES',
                    'avro_type': {
                        'type': 'bytes',
                    },
                    'transform': identity}

        case 'Edm.Boolean':
            return {'sql_type': 'BOOL',
                    'avro_type': {
                        'type': 'boolean',
                    },
                    'transform': identity}

        case 'Edm.Byte':
            return {'sql_type': 'BYTES',
                    'avro_type': {
                        'type': 'bytes',
                    },
                    'transform': identity}

        case 'Edm.Date':
            return {'sql_type': 'DATE',
                    'avro_type': {
                        'type': 'int',
                        'logicalType': 'date',
                    },
                    'transform': transform_edm_date_to_epoch_days}

        case 'Edm.DateTime':
            return {'sql_type': 'DATETIME',
                    'avro_type': {
                        'type': 'int',
                        'logicalType': 'timestamp-millis',
                    },
                    'transform': transform_edm_date_to_millis}

        case 'Edm.DateTimeOffset':
            return {'sql_type': 'DATETIME',
                    'avro_type': {
                        'type': 'int',
                        'logicalType': 'timestamp-millis',
                    },
                    'transform': transform_edm_date_to_millis}

        case 'Edm.Decimal':
            return {'sql_type': 'FLOAT64',
                    'avro_type': {
                        'type': ['null', 'double']
                    },
                    'transform': identity}

        case 'Edm.Double':
            return {'sql_type': 'FLOAT64',
                    'avro_type': {
                        'type': 'double',
                    },
                    'transform': identity}

        case 'Edm.Duration':
            return {'sql_type': 'INTERVAL',
                    'avro_type': {
                        'name': 'interval',
                        'type': 'fixed',
                        'logicalType': 'duration',
                        'size': 12,
                    },
                    'transform': identity,
                    }

        case 'Edm.Guid':
            return {'sql_type': 'STRING',
                    'avro_type': {
                        'type': 'string',
                        'logicalType': 'uuid',
                    },
                    'transform': identity}

        case 'Edm.Int16':
            return {'sql_type': 'SMALLINT',
                    'avro_type': {
                        'name': 'int16',
                        'type': 'fixed',
                        'size': 2,
                    },
                    'transform': identity}

        case 'Edm.Int32':
            return {'sql_type': 'INT',
                    'avro_type': {
                        'type': 'int',
                    },
                    'transform': identity}

        case 'Edm.Int64':
            return {'sql_type': 'BIGINT',
                    'avro_type': {
                        'type': 'long',
                    },
                    'transform': identity}

        case 'Edm.SByte':
            return {'sql_type': 'TINYINT',
                    'avro_type': {
                        'name': 'sbyte',
                        'type': 'fixed',
                        'size': 1,
                    },
                    'transform': identity}

        case 'Edm.Single':
            return {'sql_type': 'FLOAT64',
                    'avro_type': {
                        'type': 'float',
                    },
                    'transform': identity}

        case 'Edm.String':
            return {'sql_type': 'STRING',
                    'avro_type': {
                        'type': 'string',
                    },
                    'transform': identity}

        case 'Edm.Geography':
            return {'sql_type': 'GEOGRAPHY',
                    'avro_type': {
                        'type': {
                            "type": "string",
                            "logicaltype": "geography_wkt",
                        },
                    },
                    'transform': identity}

        case 'Edm.GeographyPoint':
            return {'sql_type': 'GEOGRAPHY',
                    'avro_type': {
                        'type': {
                            "type": "string",
                            "logicaltype": "geography_wkt",
                        },
                    },
                    'transform': transform_edm_point_to_pointliteral}

        case 'Edm.GeographyLineString':
            return {'sql_type': 'GEOGRAPHY',
                    'avro_type': {
                        'type': {
                            "type": "string",
                            "logicaltype": "geography_wkt",
                        },
                        'logicalType': 'linestring',
                    },
                    'transform': transform_edm_linestring_to_linestring}

        case 'Edm.GeographyMultiPoint':
            return {'sql_type': 'GEOGRAPHY',
                    'avro_type': {
                        'type': {
                            "type": "string",
                            "logicaltype": "geography_wkt",
                        },
                        'logicalType': 'multipoint',
                    },
                    'transform': transform_edm_multipoint_to_multipoint}

        case 'Edm.GeographyMultiLineString':
            return {'sql_type': 'GEOGRAPHY',
                    'avro_type': {
                        'type': {
                            "type": "string",
                            "logicaltype": "geography_wkt",
                        },
                    },
                    'transform': transform_edm_multiline_to_multilineliteral}

        case 'Edm.GeographyMultiPolygon':
            return {'sql_type': 'GEOGRAPHY',
                    'avro_type': {
                        'type': {
                            "type": "string",
                            "logicaltype": "geography_wkt",
                        },
                    },
                    'transform': transform_edm_multipolygon_to_multipolygon}

        case _:
            raise NotImplementedError(edm_type)


def merge_property(metadata, schema, prop):
    """Converts the property into a SQL schema.

    May add multiple properties if there are complex ones.
    """
    prop_name = prop.get('Name')
    prop_type = prop.get('Type')
    if prop_type.startswith('Edm.'):
        # Simple type. All good.
        schema[prop_name] = edm_to_schema_type(prop)
    elif prop_type.startswith(SOCRATA_PREFIX):
        sub_schema = complex_type_to_schema(metadata,
                                            prop_type[len(SOCRATA_PREFIX):])
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
            schema_type = edm_to_schema_type(prop)
            prop_name = prop.get('Name')

            if prop_name == '__id':
                schema_type['avro_type']['type'] = 'string'
            schema[prop_name] = schema_type

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
            merge_property(metadata, cur_schema, prop)

        fourfour_id = entity_type.get('Name')
        schemas[fourfour_id] = cur_schema

    return schemas


def get_entity_sets(metadata):
    entity_sets = []
    for element in metadata.findall('.//edm:EntityContainer[@Name="Service"]'
                                    '/edm:EntitySet',
                                    ODATA_NS):
        entity_sets.append(element.get('Name'))
    return entity_sets

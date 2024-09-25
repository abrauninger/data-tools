import functools

ODATA_NS = {'edmx': 'http://docs.oasis-open.org/odata/ns/edmx',
            'edm': 'http://docs.oasis-open.org/odata/ns/edm'}

SOCRATA_PREFIX = 'socrata.'


def edmBinaryToBytes(indata):
    raise NotImplementedError()


def edmBooleanToBool(indata):
    raise NotImplementedError()


def edmByteToBytes(indata):
    raise NotImplementedError()


def edmDateToDate(indata):
    raise NotImplementedError()


def edmDateTimeToDateTime(indata):
    raise NotImplementedError()


def edmDecimalToDecimal(indata):
    raise NotImplementedError()


def edmDoubleToDouble(indata):
    raise NotImplementedError()


def edmDoubleToFloat(indata):
    raise NotImplementedError()


def edmDurationToInterval(indata):
    raise NotImplementedError()


def edmGuidToString(indata):
    raise NotImplementedError()


def edmInt16ToSmallInt(indata):
    raise NotImplementedError()


def edmInt32ToInt(indata):
    raise NotImplementedError()


def edmInt64ToBigInt(indata):
    raise NotImplementedError()


def edmSByteToTinyInt(indata):
    raise NotImplementedError()


def edmSingleToFloat(indata):
    raise NotImplementedError()


def edmStreamToFoo(indata):
    raise NotImplementedError()


def edmStringToString(indata):
    raise NotImplementedError()


def edmTimeOfDayToFoo(indata):
    raise NotImplementedError()


def edmGeographyToGeography(indata):
    raise NotImplementedError()


def edmGeographyPointToPoint(indata):
    raise NotImplementedError()


def edmGeographyLineStringToLineString(indata):
    raise NotImplementedError()


def edmGeographyPolygonToPolygon(indata):
    raise NotImplementedError()


def edmGeographyMultiPointToMultipoint(indata):
    raise NotImplementedError()


def edmGeographyMultiLineStringToMultiLineString(indata):
    raise NotImplementedError()


def edmGeographyMultiPolygonToMultiPolygon(indata):
    raise NotImplementedError()


def edm_to_js_type(edm_node):
    edm_type = edm_node.get('Type')
    match edm_type:
        case 'Edm.Binary':
            return {'sql_type': 'BYTES',
                    'avro_type': {
                        'type': ['null', 'bytes'],
                    },
                    'transform': edmBinaryToBytes}

        case 'Edm.Boolean':
            return {'sql_type': 'BOOL',
                    'avro_type': {
                        'type': ['null', 'boolean'],
                    },
                    'transform': edmBooleanToBool}

        case 'Edm.Byte':
            return {'sql_type': 'BYTES',
                    'avro_type': {
                        'type': ['null', 'bytes'],
                    },
                    'transform': edmByteToBytes}

        case 'Edm.Date':
            return {'sql_type': 'DATE',
                    'avro_type': {
                        'type': ['null', 'int'],
                        'logicalType': 'date',
                    },
                    'transform': edmDateToDate}

        case 'Edm.DateTime':
            return {'sql_type': 'DATETIME',
                    'avro_type': {
                        'type': ['null', 'int'],
                        'logicalType': 'timestamp-millis',
                    },
                    'transform': edmDateTimeToDateTime}

        case 'Edm.DateTimeOffset':
            return {'sql_type': 'DATETIME',
                    'avro_type': {
                        'type': ['null', 'int'],
                        'logicalType': 'timestamp-millis',
                    },
                    'transform': edmDateTimeToDateTime}

        case 'Edm.Decimal':
            return {'sql_type': 'FLOAT64',
                    'avro_type': {
                        'type': ['null', 'double']
                    },
                    'transform': edmDecimalToDecimal}

        case 'Edm.Double':
            return {'sql_type': 'FLOAT64',
                    'avro_type': {
                        'type': ['null', 'double'],
                    },
                    'transform': edmDoubleToDouble}

        case 'Edm.Duration':
            return {'sql_type': 'INTERVAL',
                    'avro_type': {
                        'name': 'interval',
                        'type': ['null', 'fixed'],
                        'logicalType': 'duration',
                        'size': 12,
                    },
                    'transform': edmDurationToInterval,
                    }

        case 'Edm.Guid':
            return {'sql_type': 'STRING',
                    'avro_type': {
                        'type': ['null', 'string'],
                        'logicalType': 'uuid',
                    },
                    'transform': edmGuidToString}

        case 'Edm.Int16':
            return {'sql_type': 'SMALLINT',
                    'avro_type': {
                        'name': 'int16',
                        'type': ['null', 'fixed'],
                        'size': 2,
                    },
                    'transform': edmInt16ToSmallInt}

        case 'Edm.Int32':
            return {'sql_type': 'INT',
                    'avro_type': {
                        'type': ['null', 'int'],
                    },
                    'transform': edmInt32ToInt}

        case 'Edm.Int64':
            return {'sql_type': 'BIGINT',
                    'avro_type': {
                        'type': ['null', 'long'],
                    },
                    'transform': edmInt64ToBigInt}

        case 'Edm.SByte':
            return {'sql_type': 'TINYINT',
                    'avro_type': {
                        'name': 'sbyte',
                        'type': ['null', 'fixed'],
                        'size': 1,
                    },
                    'transform': edmSByteToTinyInt}

        case 'Edm.Single':
            return {'sql_type': 'FLOAT64',
                    'avro_type': {
                        'type': ['null', 'float'],
                    },
                    'transform': edmSingleToFloat}

        case 'Edm.String':
            return {'sql_type': 'STRING',
                    'avro_type': {
                        'type': ['null', 'string'],
                    },
                    'transform': edmStringToString}

        case 'Edm.Geography':
            return {'sql_type': 'GEOGRAPHY',
                    'avro_type': {
                        'type': ['null', 'string'],
                        'logicalType': 'geography',
                    },
                    'transform': edmGeographyToGeography}

        case 'Edm.GeographyPoint':
            return {'sql_type': 'POINT',
                    'avro_type': {
                        'type': ['null', 'string'],
                        'logicalType': 'point',
                    },
                    'transform': edmGeographyPointToPoint}

        case 'Edm.GeographyLineString':
            return {'sql_type': 'LineString',
                    'avro_type': {
                        'type': ['null', 'string'],
                        'logicalType': 'linestring',
                    },
                    'transform': edmGeographyPointToPoint}

        case 'Edm.GeographyMultiPoint':
            return {'sql_type': 'MULTIPOINT',
                    'avro_type': {
                        'type': ['null', 'string'],
                        'logicalType': 'multipoint',
                    },
                    'transform': edmGeographyMultiPointToMultipoint}

        case 'Edm.GeographyMultiLineString':
            return {'sql_type': 'MULTILINESTRING',
                    'avro_type': {
                        'type': ['null', 'string'],
                        'logicalType': 'multilinestring',
                    },
                    'transform': edmGeographyMultiLineStringToMultiLineString}

        case 'Edm.GeographyMultiPolygon':
            return {'sql_type': 'MULTIPOLYGON',
                    'avro_type': {
                        'type': ['null', 'string'],
                        'logicalType': 'multipolygon',
                    },
                    'transform': edmGeographyMultiPolygonToMultiPolygon}

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
        schema_type = edm_to_js_type(prop)
        if prop_name == '__id':
            schema_type['avro_type']['type'] = 'string'

        schema[prop_name] = schema_type
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
            schema_type = edm_to_js_type(prop)
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

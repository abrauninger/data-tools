def edmBinaryToBytes(indata):
    return indata


def edmBooleanToBool(indata):
    return indata


def edmByteToBytes(indata):
    return indata


def edmDateToDate(indata):
    return indata


def edmDateTimeToDateTime(indata):
    return indata


def edmDateTimeOffsetToFoo(indata):
    return indata


def edmDecimalToDecimal(indata):
    return indata


def edmDoubleToFloat64(indata):
    return indata


def edmDurationToInterval(indata):
    return indata


def edmGuidToString(indata):
    return indata


def edmInt16ToSmallInt(indata):
    return indata


def edmInt32ToInt(indata):
    return indata


def edmInt64ToBigInt(indata):
    return indata


def edmSByteToTinyInt(indata):
    return indata


def edmSingleToFloat64(indata):
    return indata


def edmStreamToFoo(indata):
    return indata


def edmStringToString(indata):
    return indata


def edmTimeOfDayToFoo(indata):
    return indata


def edmGeographyToGeography(indata):
    return indata


def edmGeographyPointToPoint(indata):
    return indata


def edmGeographyLineStringToLineString(indata):
    return indata


def edmGeographyPolygonToPolygon(indata):
    return indata


def edmGeographyMultiPointToMultipoint(indata):
    return indata


def edmGeographyMultiLineStringToMultiLineString(indata):
    return indata


def edmGeographyMultiPolygonToMultiPolygon(indata):
    return indata


def edm_to_js_type(edm_type):
    match edm_type:
        case 'Edm.Binary':
            return {'sql_type': 'BYTES',
                    'transform': edmBinaryToBytes}

        case 'Edm.Boolean':
            return {'sql_type': 'BOOL',
                    'transform': edmBooleanToBool}

        case 'Edm.Byte':
            return {'sql_type': 'BYTES',
                    'transform': edmByteToBytes}

        case 'Edm.Date':
            return {'sql_type': 'DATE',
                    'transform': edmDateToDate}

        case 'Edm.DateTime':
            return {'sql_type': 'DATETIME',
                    'transform': edmDateTimeToDateTime}

        case 'Edm.DateTimeOffset':
            return {'sql_type': 'FOO',
                    'transform': edmDateTimeOffsetToFoo}

        case 'Edm.Decimal':
            return {'sql_type': 'DECIMAL',
                    'transform': edmDecimalToDecimal}

        case 'Edm.Double':
            return {'sql_type': 'FLOAT64',
                    'transform': edmDoubleToFloat64}

        case 'Edm.Duration':
            return {'sql_type': 'INTERVAL',
                    'transform': edmDurationToInterval}

        case 'Edm.Guid':
            return {'sql_type': 'STRING',
                    'transform': edmGuidToString}

        case 'Edm.Int16':
            return {'sql_type': 'SMALLINT',
                    'transform': edmInt16ToSmallInt}

        case 'Edm.Int32':
            return {'sql_type': 'INT',
                    'transform': edmInt32ToInt}

        case 'Edm.Int64':
            return {'sql_type': 'BIGINT',
                    'transform': edmInt64ToBigInt}

        case 'Edm.SByte':
            return {'sql_type': 'TINYINT',
                    'transform': edmSByteToTinyInt}

        case 'Edm.Single':
            return {'sql_type': 'FLOAT64',
                    'transform': edmSingleToFloat64}

        case 'Edm.Stream':
            return {'sql_type': 'FOO',
                    'transform': edmStreamToFoo}

        case 'Edm.String':
            return {'sql_type': 'STRING',
                    'transform': edmStringToString}

        case 'Edm.TimeOfDay':
            return {'sql_type': 'FOO',
                    'transform': edmTimeOfDayToFoo}

        case 'Edm.Geography':
            return {'sql_type': 'GEOGRAPHY',
                    'transform': edmGeographyToGeography}

        case 'Edm.GeographyPoint':
            return {'sql_type': 'POINT',
                    'transform': edmGeographyPointToPoint}

        case 'Edm.GeographyLineString':
            return {'sql_type': 'LINESTRING',
                    'transform': edmGeographyLineStringToLineString}

        case 'Edm.GeographyPolygon':
            return {'sql_type': 'POLYGON',
                    'transform': edmGeographyPolygonToPolygon}

        case 'Edm.GeographyMultiPoint':
            return {'sql_type': 'MULTIPOINT',
                    'transform': edmGeographyMultiPointToMultipoint}

        case 'Edm.GeographyMultiLineString':
            return {'sql_type': 'MULTILINESTRING',
                    'transform': edmGeographyMultiLineStringToMultiLineString}

        case 'Edm.GeographyMultiPolygon':
            return {'sql_type': 'MULTIPOLYGON',
                    'transform': edmGeographyMultiPolygonToMultiPolygon}

        case _:
            raise ValueError(edm_type)

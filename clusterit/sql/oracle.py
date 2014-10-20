import decimal
from urlparse import parse_qs, urlparse
from flask import current_app, request
from geoalchemy2.elements import WKTElement
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.sql import and_, func, select, true
from shapely.geometry import box
from shapely import wkb
from clusterit.feature import Feature


def get_connection(id, config):
    connection = current_app.extensions['clusterit']['sql'].get(id, None)
    if not connection:
        user = ''
        if config.get('user'):
            user = config['user']
        if config.get('password'):
            user += ':' + config['password']
        if len(user) > 0:
            user += '@'

        connection_string = 'oracle+cx_oracle://%(user)s%(host)s/%(database)s' % {
            'user': user,
            'host': config['host'] if config['host'] else '',
            'database': config['database']
        }

        current_app.logger.info('SQL connection: %s' % connection_string)

        engine = create_engine(connection_string, echo=current_app.config['DEBUG'])
        metadata = MetaData(engine)
        table = Table(config['table'], metadata, autoload=True, oracle_resolve_synonyms=True)

        connection = {
            'engine': engine,
            'metadata': metadata,
            'table': table
        }

        current_app.extensions['clusterit']['sql'][id] = connection

    return connection

def get_features(id, config, bbox):
    connection = get_connection(id, config)

    srs = config.get('srs', 4326)
    bbox = box(*bbox).wkt

    # Get SELECT columns
    column_names = config.get('columns', [])
    for c in ['aggregation', 'aggregation_backref']:
        if config.get(c):
            columns = config[c] if isinstance(config[c], list) else [config[c]]
            for c1 in columns:
                if c1 not in column_names:
                    column_names.append(c1)

    columns = [getattr(connection['table'].c, c) for c in column_names]
    the_geom = getattr(connection['table'].c, config['geometryName'])
    # @todo: CRS transform
    columns.append(func.SDO_UTIL.TO_WKBGEOMETRY(the_geom).label(config['geometryName']))

    # Get Spatial WHERE
    spatial_filter = func.SDO_FILTER(the_geom, func.SDO_GEOMETRY(bbox, srs), 'mask=anyinteract querytype=WINDOW')

    # Get Properties filter
    property_filter = []
    for k, v in parse_qs(urlparse(request.url).query, True).items():
        if k == 'resolution' or k == 'bbox':
            continue
        if k in config.get('filter', {}):
            operand = config['filter'][k]['operand']
            operator = config['filter'][k]['operator']

            column = getattr(connection['table'].c, operand)
            value = cast(v, column.type)
            property_filter.append(getattr(column, operator)(value))

    # Get Final Query
    if len(property_filter) == 1:
        query = select(columns).where(and_(spatial_filter == 'TRUE', property_filter[0]))
    elif len(property_filter) > 1:
        query = select(columns).where(and_(spatial_filter == 'TRUE', and_(*property_filter)))
    else:
        query = select(columns).where(spatial_filter == 'TRUE')

    # Collect Features from query
    features = []
    for row in connection['engine'].connect().execute(query):
        properties = {}
        for column in columns:
            k = column.name
            v = row[k]

            if k == config['geometryName']:
                geometry = wkb.loads(v.read())
                break
            elif isinstance(v, decimal.Decimal):
                v = float(v)

            properties[k] = v
        features.append(Feature(geometry, properties))

    return features

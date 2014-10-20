from json import loads

from geojson.codec import dumps

from . import sql


class Cluster(object):
    def __init__(self, feature, threshold=1, resolution=1, use_centroid=False,
                 aggregation=False, aggregation_split=None,
                 aggregation_backref=None, include_features=False):
        self.features = [feature]
        self.threshold = threshold
        self.resolution = resolution
        self.use_centroid = use_centroid
        self.aggregation = aggregation
        self.aggregation_split = aggregation_split
        self.aggregation_backref = aggregation_backref
        self.include_features = include_features

    def add(self, feature):
        if self.features[0].geometry.distance(feature.geometry) / self.resolution <= self.threshold:
            self.features.append(feature)
            return True
        return False

    def _get_backref(self, feature):
        if self.include_features:
            backref = feature.geoJSON()
        elif isinstance(self.aggregation_backref, basestring):
            backref = feature.properties.get(self.aggregation_backref)
        elif isinstance(self.aggregation_backref, list):
            backref = {}
            for attribute in self.aggregation_backref:
                backref[attribute] = feature.properties.get(attribute)
        else:
            backref = None

        return backref

    def geoJSON(self):
        geometry = self.features[0].geometry.centroid
        if self.use_centroid:
            for feature in self.features[1:]:
                geometry = geometry.union(feature.geometry)
            geometry = geometry.centroid

        properties = {
            'count': len(self.features)
        }

        if self.aggregation:
            properties['aggregation'] = {}
            for feature in self.features:
                key = feature.properties.get(self.aggregation)
                if isinstance(key, list):
                    keys = key
                    if self.aggregation_split:
                        keys = [key.split(self.aggregation_split) for key in keys]
                else:
                    if key and self.aggregation_split:
                        keys = key.split(self.aggregation_split)
                    else:
                        keys = [key]

                for key in keys:
                    if key in properties['aggregation']:
                        if self.aggregation_backref:
                            properties['aggregation'][key]['count'] += 1
                            properties['aggregation'][key]['backrefs'].append(self._get_backref(feature))
                        else:
                            properties['aggregation'][key] += 1
                    else:
                        if self.aggregation_backref:
                            properties['aggregation'][key] = {
                                'count': 1,
                                'backrefs': [self._get_backref(feature)]
                            }
                        else:
                            properties['aggregation'][key] = 1

        if self.include_features and not (self.aggregation and self.aggregation_backref):
                properties['features'] = [f.geoJSON() for f in self.features]

        return {
            'type': 'Feature',
            'geometry': loads(dumps(geometry)),
            'properties': properties
        }


def get_clusters(id, config, bbox, resolution):
    conn_type = config['type'].lower()
    if  conn_type in ['sql', 'sql+postgis']:
        import sql.postgis
        features = sql.postgis.get_features(id, config, bbox)
    elif conn_type == 'sql+oracle':
        import sql.oracle
        features = sql.oracle.get_features(id, config, bbox)
    else:
        features = []

    return cluster_features(
        features,
        threshold=config.get('threshold', 1),
        resolution=resolution,
        use_centroid=config.get('use_centroid'),
        aggregation=config.get('aggregation'),
        aggregation_split=config.get('aggregation_split'),
        aggregation_backref=config.get('aggregation_backref'),
        include_features=config.get('include_features'))


def cluster_features(features, threshold, resolution=1, use_centroid=False,
                     aggregation=False, aggregation_split=None,
                     aggregation_backref=None, include_features=False):
    clusters = []
    for feature in features:
        clustered = False
        for cluster in reversed(clusters):
            if cluster.add(feature):
                clustered = True
                break
        if not clustered:
            clusters.append(Cluster(
                feature, threshold, resolution, use_centroid, aggregation,
                aggregation_split, aggregation_backref, include_features))

    return clusters

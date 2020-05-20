import copy
import dateutil.parser
import json
import random

from sandbox_exporter.flattener import DataFlattener


class WzdxV2Flattener(DataFlattener):
    '''
    Reads each WZDx specification v2 feed performs data split  and transformation,
    including:
    1) Flatten the data structure
    2) Transforming of fields to enhance usage of the dataset in Socrata
    (e.g. geometry_multipoint, geometry_linestring)

    '''
    def __init__(self, *args, **kwargs):
        super(WzdxV2Flattener, self).__init__(*args, **kwargs)
        self.json_string_fields = ['lanes', 'geometry', 'restrictions']

    def process_and_split(self, raw_rec):
        out_recs = []
        for feature in raw_rec['features']:
            temp = {k:v for k,v in feature['properties'].items()}
            temp['geometry'] = feature['geometry']
            temp['road_event_feed_info'] = raw_rec['road_event_feed_info']
            out_recs.append(temp)
        return [self.process(out_rec) for out_rec in out_recs]

    def add_enhancements(self, rec):
        geometry = json.loads(rec['geometry'])
        if geometry['type'] == 'LineString':
            coords = ', '.join(['{} {}'.format(i[0], i[1]) for i in geometry['coordinates']])
            rec['geometry_linestring'] = f'LINESTRING ({coords})'
            rec['geometry_multipoint'] = ''
        elif geometry['type'] == 'MultiPoint':
            coords = ', '.join(['({} {})'.format(i[0], i[1]) for i in geometry['coordinates']])
            rec['geometry_multipoint'] = f'MULTIPOINT ({coords})'
            rec['geometry_linestring'] = ''
        rec = {k:v if type(v) != list else json.dumps(v) for k,v in rec.items()}
        return rec

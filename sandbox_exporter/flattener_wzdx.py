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
        self.json_string_fields = ['lanes', 'geometry', 'restrictions', 'types_of_work']

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


class WzdxV3Flattener(WzdxV2Flattener):
    '''
    Reads each WZDx specification v3 feed performs data split  and transformation,
    including:
    1) Flatten the data structure
    2) Transforming of fields to enhance usage of the dataset in Socrata
    (e.g. geometry_multipoint, geometry_linestring)

    '''
    def __init__(self, *args, **kwargs):
        super(WzdxV3Flattener, self).__init__(*args, **kwargs)
        self.json_string_fields = [
            'lanes', 'geometry', 'restrictions', 'types_of_work',
            'relationship_first', 'relationship_next', 'relationship_parents', 'relationship_children'
        ]

    def create_data_source_dict(self, data_sources):
        data_source_dict = {
            data_source['data_source_id']:
            {f'data_source_{k}' if k != 'data_source_id' else k:v for k,v in data_source.items()}
            for data_source in data_sources
        }
        return data_source_dict

    def process_and_split(self, raw_rec):
        data_sources = raw_rec['road_event_feed_info']['data_sources']
        data_source_dict = self.create_data_source_dict(data_sources)
        out_recs = []
        for feature in raw_rec['features']:
            temp = {k:v for k,v in feature['properties'].items()}
            temp['geometry'] = feature['geometry']
            temp['road_event_feed_info'] = raw_rec['road_event_feed_info']
            data_source_id = feature['properties']['data_source_id']
            if data_source_id in data_source_dict:
                temp.update(data_source_dict[data_source_id])
            out_recs.append(temp)
        return [self.process(out_rec) for out_rec in out_recs]

    def add_enhancements(self, rec):
        rec = super(WzdxV3Flattener, self).add_enhancements(rec)
        del rec['road_event_feed_info_data_sources']
        del rec['geometry']
        return rec

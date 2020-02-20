import copy
import dateutil.parser
import json
import random

from sandbox_exporter.flattener import CvDataFlattener


class TheaBSMFlattener(CvDataFlattener):
    '''
    Reads each raw BSM data record from Tampa CV Pilot and performs data transformation,
    including:
    1) Flatten the data structure
    2) Rename certain fields to achieve consistency across data sets
    3) Add additional fields to enhance usage of the data set in Socrata
    (e.g. randomNum, coreData_position)

    '''
    def __init__(self, **kwargs):
        super(TheaBSMFlattener, self).__init__(**kwargs)
        self.rename_fields += [
            ('coreData_lat', 'coreData_position_lat'),
            ('coreData_long', 'coreData_position_long'),
            ('coreData_elev', 'coreData_elevation'),
            ('coreData_accelset_yaw', 'coreData_accelset_accelYaw')
        ]
        self.part2_rename_prefix_fields = [
            ('classDetails_', 'part2_suve_cd_'),
            ('vehicleData_', 'part2_suve_vd_'),
            ('vehicleAlerts_events_', 'part2_spve_vehalert_event_'),
            ('vehicleAlerts_', 'part2_spve_vehalert_'),
            ('trailers_', 'part2_spve_tr_'),
            ('description_', 'part2_spve_event_'),
            ('events_', 'part2_vse_events'),
            ('pathHistory_', 'part2_vse_ph_'),
            ('pathPrediction_', 'part2_vse_pp_'),
            ('lights_', 'part2_vse_lights'),
            ('coreData_accelSet', 'coreData_accelset_')
        ]
        self.part2_rename_fields = [
            ('classification', 'part2_suve_classification'),
            ('part2_spve_event_description', 'part2_spve_event_desc'),
            ('part2_spve_tr_connection', 'part2_spve_tr_conn'),
            ('part2_spve_tr_sspRights', 'part2_spve_tr_ssprights'),
            ('events', 'part2_vse_events'),
            ('part2_vse_pp_radiusOfCurve', 'part2_vse_pp_radiusofcurve'),
            ('part2_vse_ph_crumbData_PathHistoryPoint', 'part2_vse_ph_crumbdata'),
            ('part2_suve_cd_hpmsType', 'part2_suve_cd_hpmstype')
        ]
        self.part2_json_string_fields = ['part2_vse_ph_crumbdata']

    def process(self, raw_rec):
        '''
        	Parameters:
        		raw_rec: dictionary object of a single BSM record

        	Returns:
        		transformed dictionary object of the BSM record
        '''
        out = super(TheaBSMFlattener, self).process(raw_rec)

        if 'payload_data_partII_SEQUENCE' in out:
            for elem in out['payload_data_partII_SEQUENCE']:
                for part2_type, part2_val in elem['partII-Value'].items():
                    part2_val_out = self.transform(part2_val,
                        rename_prefix_fields=self.part2_rename_prefix_fields,
                        rename_fields=self.part2_rename_fields,
                        int_fields=[],
                        json_string_fields=self.part2_json_string_fields)
                    out.update(part2_val_out)
            del out['payload_data_partII_SEQUENCE']

        if 'coreData_position_long' in out:
            coreData_position_long = float(out['coreData_position_long'])/10e6
            coreData_position_lat = float(out['coreData_position_lat'])/10e6
            out['coreData_position'] = "POINT ({} {})".format(coreData_position_long, coreData_position_lat)

        if 'coreData_size_width' in out:
            out['coreData_size'] = json.dumps({'width': int(out['coreData_size_width']),
                                               'length': int(out['coreData_size_length'])})
            del out['coreData_size_width']
            del out['coreData_size_length']

        if 'coreData_brakes_wheelBrakes' in out:
            out['coreData_brakes_wheelBrakes_unavailable'] = out['coreData_brakes_wheelBrakes'][0]
            out['coreData_brakes_wheelBrakes_leftFront'] = out['coreData_brakes_wheelBrakes'][1]
            out['coreData_brakes_wheelBrakes_leftRear'] = out['coreData_brakes_wheelBrakes'][2]
            out['coreData_brakes_wheelBrakes_rightFront'] = out['coreData_brakes_wheelBrakes'][3]
            out['coreData_brakes_wheelBrakes_rightRear'] = out['coreData_brakes_wheelBrakes'][4]
            del out['coreData_brakes_wheelBrakes']

        return out

class TheaTIMFlattener(CvDataFlattener):
    '''
    Reads each raw TIM data record from Tampa CV Pilot and performs data transformation,
    including:
    1) Flatten the data structure
    2) Rename certain fields to achieve consistency across data sets
    3) Add additional fields to enhance usage of the data set in Socrata
    (e.g. randomNum, coreData_position)

    '''
    def __init__(self, **kwargs):
        super(TheaTIMFlattener, self).__init__(**kwargs)
        self.rename_prefix_fields += [
            ('payload_data_TravelerInformation_dataFrames_TravelerDataFrame_', 'travelerdataframe_'),
            ('payload_data_TravelerInformation_', 'travelerinformation_'),
            ('travelerdataframe_regions_GeographicalPath_description_path_', 'travelerdataframe_desc_'),
            ('travelerdataframe_regions_GeographicalPath_', 'travelerdataframe_'),
            ('_SEQUENCE', '_sequence'),
            ('_msgId_roadSignID_position_', '_msgId_'),
            ('_msgId_roadSignID_', '_msgId_')
        ]
        self.rename_fields += [
            ('travelerdataframe_desc_offset_xy_nodes_NodeXY', 'travelerdataframe_desc_nodes'),
            ('travelerdataframe_description_path_scale', 'travelerdataframe_desc_scale')
        ]
        self.json_string_fields += [
        'SEQUENCE', 'travelerdataframe_desc_nodes', 'itis'
        ]

    def process(self, raw_rec):
        '''
        	Parameters:
        		raw_rec: dictionary object of a single TIM record

        	Returns:
        		transformed dictionary object of the TIM record
        '''
        out = super(TheaTIMFlattener, self).process(raw_rec)

        if 'travelerdataframe_msgId_lat' in out:
            travelerdataframe_msgId_lat = float(out['travelerdataframe_msgId_lat'])/10e6
            travelerdataframe_msgId_long = float(out['travelerdataframe_msgId_long'])/10e6
            out['travelerdataframe_msgId_position'] = "POINT ({} {})".format(travelerdataframe_msgId_long, travelerdataframe_msgId_lat)

        return out

    def process_and_split(self, raw_rec):
        out_recs = []
        try:
            tdfs = copy.deepcopy(raw_rec['payload']['data']['TravelerInformation']['dataFrames']['TravelerDataFrame'])
            if type(tdfs) == list:
                for tdf in tdfs:
                    temp_rec = copy.deepcopy(raw_rec)
                    temp_rec['payload']['data']['TravelerInformation']['dataFrames']['TravelerDataFrame'] = tdf
                    out_recs.append(temp_rec)
            else:
                out_recs.append(raw_rec)
        except:
            out_recs.append(raw_rec)
        return [self.process(out_rec) for out_rec in out_recs if out_rec]

class TheaSPATFlattener(CvDataFlattener):
    '''
    Reads each raw SPaT data record from Tampa CV Pilot and performs data transformation,
    including:
    1) Flatten the data structure
    2) Rename certain fields to achieve consistency across data sets
    3) Add additional fields to enhance usage of the data set in Socrata
    (e.g. randomNum, coreData_position)

    '''
    def __init__(self, **kwargs):
        super(TheaSPATFlattener, self).__init__(**kwargs)

        self.json_string_fields += [
        'MovementState'
        ]

    def process(self, raw_rec):
        '''
        	Parameters:
        		raw_rec: dictionary object of a single SPaT record

        	Returns:
        		transformed dictionary object of the SPaT record
        '''
        out = super(TheaSPATFlattener, self).process(raw_rec)

        return out

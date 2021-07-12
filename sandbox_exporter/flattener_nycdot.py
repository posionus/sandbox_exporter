import copy

from sandbox_exporter.flattener import CvDataFlattener


class NycdotEVENTFlattener(CvDataFlattener):
    '''
    Reads each raw EVENT data record from NYCDOT CV Pilot and performs data transformation,
    including:
    1) Flatten the data structure
    2) Add additional fields to enhance usage of the data set in Socrata
    (e.g. randomNum, coreData_position)

    '''
    def __init__(self, **kwargs):
        super(NycdotEVENTFlattener, self).__init__(**kwargs)
        self.json_string_fields =  ['bsmList','mapList', 'spatList', 'timList']
        self.col_order = [
            'eventHeader_eventTimeBin',
            'dayOfWeekBin',
            'timeOfDayBin',
            'eventHeader_eventLocationBin',
            'nearRSU',
            'bureauBin',
            'roadTypeBin',
            'eventHeader_locationSource',
            'eventHeader_asdFirmwareVersion',
            'eventHeader_eventAlertActive',
            'eventHeader_eventAlertSent',
            'eventHeader_eventAlertHeard',
            'eventHeader_hostVehID',
            'eventHeader_targetVehID',
            'eventHeader_triggerHVSeqNum',
            'eventHeader_triggerTVSeqNum',
            'eventHeader_eventType',
            'eventHeader_eventStatus',
            'eventHeader_grpId',
            'eventHeader_weatherCondition',
            'eventHeader_airTemperature',
            'eventHeader_precipitation1Hr',
            'eventHeader_windSpeed',
            'eventHeader_speedCondition',
            'eventHeader_lastPlowed',
            'eventHeader_parameters_recordingROI',
            'eventHeader_parameters_timeRecordBefore',
            'eventHeader_parameters_timeRecordFollow',
            'eventHeader_parameters_timeRecordResolution',
            'eventHeader_parameters_minSpdThreshold',
            'eventHeader_parameters_timeToCrash',
            'eventHeader_parameters_excessiveCurveSpd',
            'eventHeader_parameters_excessiveSpd',
            'eventHeader_parameters_excessiveSpdTime',
            'eventHeader_parameters_excessiveCurveSpdTime',
            'eventHeader_parameters_excessiveZoneSpd',
            'eventHeader_parameters_excessiveZoneSpdTime',
            'eventHeader_parameters_minCurveSpd',
            'eventHeader_parameters_minZoneSpd',
            'eventHeader_parameters_stopBarTolerance',
            'eventHeader_parameters_yellowDurationTolerance',
            'eventHeader_parameters_hardBrakingThreshold',
            'eventHeader_parameters_assumedDriverBraking',
            'eventHeader_parameters_postedHeightLimit',
            'eventHeader_parameters_postedSizeLimit',
            'eventHeader_parameters_postedZoneSpeed',
            'eventHeader_parameters_regulatorySpeed',
            'bsmList',
            'mapList',
            'spatList',
            'timList'
        ]

    def add_enhancements(self, r):
        r['yearMonthBin'] = r['eventHeader_eventTimeBin'][:7]
        r['dayOfWeekBin'], r['timeOfDayBin'] = r['eventHeader_eventTimeBin'].split('-')[-2:]
        if r['eventHeader_eventLocationBin'] != 'N/A':
            r['nearRSU'] = r['eventHeader_eventLocationBin'].split('-')[0]=='CV'
            r['bureauBin'], r['roadType'] = r['eventHeader_eventLocationBin'].split('-')[-2:]
        return r

    def process(self, raw_rec):
        '''
        	Parameters:
        		raw_rec: dictionary object of a single BSM record

        	Returns:
        		transformed dictionary object of the BSM record
        '''
        out = super(NycdotEVENTFlattener, self).process(raw_rec)

        return out


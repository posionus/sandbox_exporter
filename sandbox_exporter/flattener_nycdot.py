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

    def add_enhancements(self, r):
        r['yearMonthBin'] = r['eventHeader_eventTimeBin'][:7]
        r['dayOfWeekBin'], r['timeOfDayBin'] = r['eventHeader_eventTimeBin'].split('-')[-2:]
        if r['eventHeader_eventLocationBin'] not in {'N/A', 'nonNYC'}:
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


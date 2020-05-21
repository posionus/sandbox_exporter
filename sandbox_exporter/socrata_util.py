'''
Helper class for loading data to Socrata datasets on data.transportation.gov.

'''
import boto3
import copy
import itertools
import json
import logging
import os
import requests
from sodapy import Socrata
import time


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging


class SocrataDataset(object):
    def __init__(self, dataset_id, socrata_client=None, socrata_params={}, float_fields=[]):
        self.dataset_id = dataset_id
        self.client = socrata_client
        if not socrata_client and socrata_params:
            self.client = Socrata(**socrata_params)
        self.socrata_params = socrata_params
        self.col_dtype_dict = self.get_col_dtype_dict()
        self.float_fields = float_fields

    def get_col_dtype_dict(self):
        '''
        Retrieve data dictionary of a Socrata data set in the form of a dictionary,
        with the key being the column name and the value being the column data type

    	Returns:
    		data dictionary of a Socrata data set in the form of a dictionary,
            with the key being the column name and the value being the column data type
        '''
        dataset_col_meta = self.client.get_metadata(self.dataset_id)['columns']
        col_dtype_dict = {col['name']: col['dataTypeName'] for col in dataset_col_meta}
        return col_dtype_dict

    def mod_dtype(self, rec, col_dtype_dict=None, float_fields=None):
        '''
        Make sure the data type of each field in the data record matches the data type
        of the field in the Socrata data set.

    	Parameters:
    		rec: dictionary object of the data record
            col_dtype_dict: data dictionary of a Socrata data set in the form of a dictionary,
            with the key being the column name and the value being the column data type
            float_fields: list of fields that should be a float

    	Returns:
    		dictionary object of the data record, with number, string, and boolean fields
            modified to align with the data type of the corresponding Socrata data set
        '''
        col_dtype_dict = col_dtype_dict or self.col_dtype_dict
        float_fields = float_fields or self.float_fields

        identity = lambda x: x
        dtype_func = {'number': float, 'text': str, 'checkbox': bool}
        out = {}
        for k,v in rec.items():
            if k in float_fields and k in col_dtype_dict:
                out[k] = float(v)
            elif k in col_dtype_dict:
                if v is not None and v is not '':
                    out[k] = dtype_func.get(col_dtype_dict.get(k, 'nonexistentKey'), identity)(v)
        out = {k:v for k,v in out.items() if k in col_dtype_dict}
        return out

    def create_new_draft(self):
        draftDataset = requests.post('https://{}/api/views/{}/publication.json'.format(self.client.domain, self.dataset_id),
                                  auth=(self.socrata_params['username'], self.socrata_params['password']),
                                  params={'method': 'copySchema'})
        logger.info(draftDataset.json())
        draftId = draftDataset.json()['id']
        return draftId

    def publish_draft(self, draftId):
        time.sleep(5)
        publishResponse = requests.post('https://{}/api/views/{}/publication.json'.format(self.client.domain, draftId),
                                        auth=(self.socrata_params['username'], self.socrata_params['password']))
        logger.info(publishResponse.json())
        return publishResponse

    def delete_draft(self, draftId):
        time.sleep(5)
        deleteResponse = self.client.delete(draftId)
        if deleteResponse.status_code == 200:
            logger.info('Empty draft {} has been discarded.'.format(draftId))
        return deleteResponse

    def clean_and_upsert(self, recs, dataset_id=None):
        dataset_id = dataset_id or self.dataset_id
        out_recs = [self.mod_dtype(r) for r in recs]
        uploadResponse = self.client.upsert(dataset_id, out_recs)
        return uploadResponse

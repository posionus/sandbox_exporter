"""
Utility for retrieving ITS sandbox data.

`SandboxExporter` can be imported into your own code by adding the following statement:

`from sandbox_exporter.exporter import SandboxExporter`


This file can also be used in command line as described at the end of this file.
Run the below in command line to get more information:

`exporter.py --help`
"""
from __future__ import print_function
from argparse import ArgumentParser
from copy import copy
import dateutil.parser
from datetime import datetime, timedelta
from functools import reduce
import json
import logging
import os
import csv
import time
import zipfile

from sandbox_exporter.flattener import load_flattener, DataFlattener
from sandbox_exporter.s3 import S3Helper


class SandboxExporter(object):

    def __init__(self, bucket='usdot-its-cvpilot-public-data', log=False,
                output_convention='{pilot}_{message_type}_{sdate}_{edate}',
                aws_profile="default", verbose=False):
        # set up
        self.bucket = bucket
        self.output_convention = output_convention
        self.aws_profile = aws_profile
        self.s3helper = S3Helper(aws_profile=aws_profile, verbose=verbose)
        self.verbose = verbose
        self.print_func = print

        if log:
            logging.basicConfig(filename='sandbox_to_csv.log', format='%(asctime)s %(message)s')
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
            self.print_func = logger.info
        if not log and not verbose:
            self.print_func = lambda x: x

    def get_folder_prefix(self, pilot, message_type, dt):
        y,m,d,h = dt.strftime('%Y-%m-%d-%H').split('-')
        folder = '{}/{}/{}/{}/{}/{}'.format(pilot, message_type.upper(), y, m, d, h)
        return folder

    def write_json_newline(self, recs, fp):
        with open(fp, 'w') as outfile:
            for r in recs:
                outfile.write(json.dumps(r))
                outfile.write('\n')

    def write_csv(self, recs, fp, flattener=DataFlattener()):
        flat_recs = []
        for r in recs:
            flat_recs += flattener.process_and_split(r)

        with open(fp, mode='w') as csv_file:
            field_names = reduce(lambda x, y: set(list(x)+list(y)), flat_recs)
            writer = csv.DictWriter(csv_file, fieldnames=field_names)
            writer.writeheader()
            for flat_rec in flat_recs:
                writer.writerow(flat_rec)

    def zip_files(self, fp_params, filenames):
        if not filenames:
            return
        outfp = (self.output_convention+'.zip').format(**fp_params)
        with zipfile.ZipFile(outfp, 'w') as outzip:
            for fp in filenames:
                outzip.write(fp, compress_type=zipfile.ZIP_DEFLATED)
                os.remove(fp)
        self.print_func('Output zip file containing {} files at:\n{}'.format(len(filenames), outfp))

    def clean_dates(self, sdate, edate):
        if type(sdate) != datetime:
            sdate = dateutil.parser.parse(sdate)
        if type(edate) != datetime and edate:
            edate = dateutil.parser.parse(edate)
        elif not edate:
            edate = sdate + timedelta(hours=24)
        return sdate, edate

    def get_prefixes(self, sdate, edate, pilot=None, message_type=None):
        sdate, edate = self.clean_dates(sdate, edate)
        fp_params = {
            'pilot': pilot,
            'message_type': message_type.lower(),
            'sdate': sdate.strftime('%Y%m%d%H'),
            'edate': edate.strftime('%Y%m%d%H')
        }
        fp = lambda filenum: (self.output_convention+'_{filenum}').format(filenum=filenum, **fp_params)
        sfolder = self.get_folder_prefix(pilot, message_type, sdate)
        efolder = self.get_folder_prefix(pilot, message_type, edate)

        curr_folder = sfolder
        curr_dt = copy(sdate)
        prefixes = []
        while curr_folder < efolder:
            prefixes.append('s3://{}/{}'.format(self.bucket, curr_folder))
            curr_dt += timedelta(hours=24)
            curr_folder = self.get_folder_prefix(pilot, message_type, curr_dt)
        return prefixes

    def get_record_generator(self, sdate, edate=None, pilot=None, message_type=None,
                             limit=0, output_fields=None, where=None):
        prefixes = self.get_prefixes(sdate, edate, pilot, message_type)
        generator = self.s3helper.select(prefixes=" ".join(prefixes), limit=limit,
                                         output_fields=output_fields, where=where)
        return generator

    def get_records(self, sdate, edate=None, pilot=None, message_type=None,
                             limit=0, output_fields=None, where=None):
        prefixes = self.get_prefixes(sdate, edate, pilot, message_type)
        generator = self.s3helper.select(prefixes=" ".join(prefixes), limit=limit,
                                         output_fields=output_fields, where=where)
        records = list(generator)
        for info in self.s3helper.info:
            self.print_func(info)
        return records

    def get_count(self, sdate, edate=None, pilot=None, message_type=None,
                  output_fields=None, where=None):
        prefixes = self.get_prefixes(sdate, edate, pilot, message_type)
        generator = self.s3helper.select(prefixes=" ".join(prefixes), count=True,
                                         output_fields=output_fields, where=where)
        count = int(list(generator)[0])
        for info in self.s3helper.info:
            self.print_func(info)
        return count

    def export_to_file(self, sdate, edate=None, pilot=None, message_type=None,
                       limit=0, output_fields=None, where=None, csv=False, zip_files=False):
        t0 = time.time()
        sdate, edate = self.clean_dates(sdate, edate)
        generator = self.get_record_generator(sdate, edate, pilot, message_type,
                                            limit, output_fields, where)
        fp_params = {
            'pilot': pilot,
            'message_type': message_type.lower(),
            'sdate': sdate.strftime('%Y%m%d%H'),
            'edate': edate.strftime('%Y%m%d%H')
        }
        fp = lambda filenum: (self.output_convention+'_{filenum}').format(filenum=filenum, **fp_params)

        if csv and not output_fields:
            flattenerMod = load_flattener('{}/{}'.format(pilot, message_type.upper()))
            flattener = flattenerMod()
        else:
            flattener=DataFlattener()

        filenum = 0
        records = []
        filenames = []
        for rec in generator:
            records.append(rec)
            if len(records) > 10000:
                if csv:
                    filename = fp(filenum)+'.csv'
                    self.write_csv(records, filename, flattener)
                else:
                    filename = fp(filenum)+'.txt'
                    self.write_json_newline(records, filename)
                self.print_func('Wrote {} recs to {}'.format(len(records), filename))
                filenames.append(filename)
                records = []
                filenum += 1
        if len(records) > 0:
            if csv:
                filename = fp(filenum)+'.csv'
                self.write_csv(records, filename, flattener)
            else:
                filename = fp(filenum)+'.txt'
                self.write_json_newline(records, filename)
            self.print_func('Wrote {} recs to {}'.format(len(records), filename))
            filenames.append(filename)
            records = []
            filenum += 1

        t1 = time.time()
        self.print_func('===========================')

        if zip_files:
            self.zip_files(fp_params, filenames)
        else:
            self.print_func('Output files:\n{}'.format('\n'.join(filenames)))

        for info in self.s3helper.info:
            self.print_func(info)
        self.print_func('Process took {} minutes'.format((t1-t0)/60))
        self.print_func('============END============')


if __name__ == '__main__':
    """
    usage: exporter.py [-h] [--bucket BUCKET] [--pilot PILOT]
                       [--message_type MESSAGE_TYPE] --sdate SDATE [--edate EDATE]
                       [--output_convention OUTPUT_CONVENTION] [--json]
                       [--aws_profile AWS_PROFILE] [--zip] [--log] [--verbose]
                       [--limit LIMIT] [--output_fields OUTPUT_FIELDS]
                       [--where WHERE]

    Script for exporting ITS sandbox data from specified date range to merged CSV
    files.

    optional arguments:
      -h, --help            show this help message and exit
      --bucket BUCKET       Name of the s3 bucket. Default: usdot-its-cvpilot-
                            public-data
      --pilot PILOT         Pilot name (options: wydot, thea). Default: wydot
      --message_type MESSAGE_TYPE
                            Message type (options: bsm, tim, spat). Default: tim
      --sdate SDATE         Starting generatedAt date of your data, in the format
                            of YYYY-MM-DD.
      --edate EDATE         Ending generatedAt date of your data, in the format of
                            YYYY-MM-DD. If not supplied, this will be set to 24
                            hours from the start date.
      --output_convention OUTPUT_CONVENTION
                            Supply string for naming convention of output file.
                            Variables available for use in this string include:
                            pilot, messate_type, sdate, edate. Note that a file
                            number will always be appended to the output file
                            name. Default: {pilot}_{message_type}_{sdate}_{edate}
      --json                Supply flag if file is to be exported as newline json
                            instead of CSV file. Default: False
      --aws_profile AWS_PROFILE
                            Supply name of AWS profile if not using default
                            profile. AWS profile must be configured in
                            ~/.aws/credentials on your machine. See https://boto3.
                            amazonaws.com/v1/documentation/api/latest/guide/config
                            uration.html#shared-credentials-file for more
                            information.
      --zip                 Supply flag if output files should be zipped together.
                            Default: False
      --log                 Supply flag if script progress should be logged and
                            not printed to the console. Default: False
      --verbose             Supply flag if script progress should be verbose.
                            Default: False
      --limit LIMIT         Maximum number of results to return. Default: no limit
      --output_fields OUTPUT_FIELDS
                            What fields or columns to output. Supply a comma
                            delimited string of the field names, assuming that the
                            record is 's'. For example, if you want to retrieve
                            fields 'metadata' and 'payload.coreData' only, supply
                            's.metadata,s.payload.coreData'. Default: all fields
                            will be returned.
      --where WHERE         WHERE part of the SQL query. Assume that the record is
                            's'. For example, a query could look like this:
                            s.metadata.bsmSource='RV' and
                            s.payload.data.coreData.speed < 15. Default: not
                            supply a WHERE clause.

    Sample Usage
    Retrieve all WYDOT TIM data from 2019-09-16:
    python -u sandbox_exporter/exporter.py --pilot wydot --message_type bsm --sdate 2020-01-22

    Retrieve all WYDOT TIM data between 2020-01-22 to 2019-09-18:
    python -u sandbox_exporter/exporter.py --pilot wydot --message_type bsm --sdate 2020-01-22

    Retrieve all WYDOT TIM data between 2020-01-22 to 2020-01-24 in json newline format (instead of flattened CSV),
    only retrieving the metadata field:
    python -u sandbox_exporter/exporter.py --pilot wydot --message_type bsm --sdate 2020-01-22 --edate 2020-01-24 --json --output_fields 's.metadata' --verbose
    """

    parser = ArgumentParser(description="Script for exporting ITS sandbox data from specified date range to merged CSV files")
    parser.add_argument('--bucket', default="test-usdot-its-cvpilot-public-data", help="Name of the s3 bucket. Default: usdot-its-cvpilot-public-data")
    parser.add_argument('--pilot', default="wydot", help="Pilot name (options: wydot, thea). Default: wydot")
    parser.add_argument('--message_type', default="tim", help="Message type (options: bsm, tim, spat). Default: tim")
    parser.add_argument('--sdate', default=None, required=True, help="Starting generatedAt date of your data, in the format of YYYY-MM-DD.")
    parser.add_argument('--edate', default=None, help="Ending generatedAt date of your data, in the format of YYYY-MM-DD. If not supplied, this will be set to 24 hours from the start date.")
    parser.add_argument('--output_convention', default='{pilot}_{message_type}_{sdate}_{edate}', help="Supply string for naming convention of output file. Variables available for use in this string include: pilot, messate_type, sdate, edate. Note that a file number will always be appended to the output file name. Default: {pilot}_{message_type}_{sdate}_{edate}")
    parser.add_argument('--json', default=False, action='store_true', help="Supply flag if file is to be exported as newline json instead of CSV file. Default: False")
    parser.add_argument('--aws_profile', default='default', help="Supply name of AWS profile if not using default profile. AWS profile must be configured in ~/.aws/credentials on your machine. See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file for more information.")
    parser.add_argument('--zip', default=False, action='store_true', help="Supply flag if output files should be zipped together. Default: False")
    parser.add_argument('--log', default=False, action='store_true', help="Supply flag if script progress should be logged and not printed to the console. Default: False")
    parser.add_argument('--verbose', default=False, action='store_true', help="Supply flag if script progress should be verbose. Default: False")
    parser.add_argument('--limit', type=int, default=0, help="Maximum number of results to return. Default: no limit")
    parser.add_argument('--output_fields', default=None, help="What fields or columns to output. Supply a comma delimited string of the field names, assuming that the record is 's'. For example, if you want to retrieve fields 'metadata' and 'payload.coreData' only, supply 's.metadata,s.payload.coreData'. Default: all fields will be returned.")
    parser.add_argument('--where', default=None, help="WHERE part of the SQL query. Assume that the record is 's'. For example, a query could look like this: s.metadata.bsmSource='RV' and s.payload.data.coreData.speed < 15. Default: not supply a WHERE clause.")
    args = parser.parse_args()

    exporter = SandboxExporter(
        bucket=args.bucket,
        log=args.log,
        output_convention=args.output_convention,
        aws_profile=args.aws_profile,
        verbose=args.verbose
    )

    exporter.export_to_file(
        sdate=args.sdate,
        edate=args.edate,
        pilot=args.pilot,
        message_type=args.message_type,
        limit=args.limit,
        output_fields=args.output_fields,
        where=args.where,
        csv=bool(not args.json),
        zip_files=args.zip,
    )

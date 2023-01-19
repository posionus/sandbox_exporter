"""
AWS and AWS S3 Helper functions.

"""
import boto3
import botocore.exceptions
from datetime import datetime
from gzip import GzipFile
from io import TextIOWrapper
import json
import os
import re
import traceback
import uuid
from subprocess import Popen, PIPE


class AWSHelper(object):
    """
    Helper class for connecting to AWS.

    """
    def __init__(self, aws_profile=None, logger=False, verbose=False):
        """
        Initialization function of the AWS_helper class.

        Parameters:
            aws_profile: Optional string name of your AWS profile, as set up in
                the credential file at ~/.aws/credentials. No need to pass in
                this parameter if you will be using your default profile. For
                additional information on how to set up the credential file, see
                https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_profiles.html
            logger: Optional parameter. Could pass in a logger object or not pass
                in anything. If a logger object is passed in, information will be
                logged instead of printed. If not, information will be printed.
        """
        self.aws_profile = aws_profile
        self.print_func = lambda x: print(x, flush=True)
        if logger:
            self.print_func = logger.info
        self.session = self._create_aws_session()
        if not logger and not verbose:
            self.print_func = lambda x: None
        self.verbose = verbose

    def _create_aws_session(self):
        """
        Creates AWS session using aws profile name passed in or using aws
        credentials in environment variables.

        Returns:
            AWS session object.
        """
        try:
            if self.aws_profile:
                session = boto3.session.Session(profile_name=self.aws_profile)
            else:
                session = boto3.session.Session()
        except botocore.exceptions.ProfileNotFound:
            self.print_func('Please supply a valid AWS profile name.')
            raise
        except:
            self.print_func(traceback.format_exc())
            self.print_func('Exiting. Unable to establish AWS session with the following profile name: {}'.format(self.aws_profile))
            raise
        return session


class S3Helper(AWSHelper):
    """
    Helper class for connecting to and working with AWS S3.

    """
    def __init__(self, *args, **kwargs):
        """
        Initialization function of the S3Helper class.

        """
        super(S3Helper, self).__init__(*args, **kwargs)
        self.client = self._get_client()
        self.info = []
        self.err_lines = []
        self.queue_timeout = 10

    def _get_client(self):
        """
        Creates S3 client.

        Returns:
            AWS S3 client.
        """
        return self.session.client('s3')

    def path_exists(self, bucket, path):
        """
        Check if S3 path exists.

        Parameters:
            bucket: name of S3 bucket
            path: key of S3 path

        Returns:
            Boolean (True/False)
        """
        try:
            self.client.get_object(Bucket=bucket, Key=path)
            return True
        except self.client.exceptions.NoSuchKey:
            return False

    def get_fps_from_event(self, event):
        bucket_key_tuples = [(e['s3']['bucket']['name'], e['s3']['object']['key']) for e in event['Records']]
        bucket_key_dict = {os.path.join(bucket, key): (bucket, key) for bucket, key in bucket_key_tuples}
        bucket_key_tuples_deduped = list(bucket_key_dict.values())
        return bucket_key_tuples_deduped

    def get_fps_from_prefix(self, bucket, prefix, limit=0):
        s3_source_kwargs = dict(Bucket=bucket, Prefix=prefix)

        bucket_key_tuples = []
        while True:
            resp = self.client.list_objects_v2(**s3_source_kwargs)
            if not resp.get('Contents'):
                return []
            bucket_key_tuples += [(bucket, i['Key']) for i in resp['Contents']]
            if not resp.get('NextContinuationToken'):
                break
            s3_source_kwargs['ContinuationToken'] = resp['NextContinuationToken']
            if limit > 0 and len(bucket_key_tuples) > limit:
                break
        return bucket_key_tuples

    def get_fp_chunks_from_prefix(self, s3_source_kwargs):
        '''
        initial s3_source_kwargs looks like:
        dict(
            Bucket=bucket,
            Prefix=prefix
        )
        '''
        resp = self.client.list_objects_v2(**s3_source_kwargs)
        if not resp.get('Contents'):
            return [], None
        bucket_key_tuples = [(s3_source_kwargs['Bucket'], i['Key']) for i in resp['Contents']]
        if not resp.get('NextContinuationToken'):
            return bucket_key_tuples, None
        s3_source_kwargs['ContinuationToken'] = resp['NextContinuationToken']
        return bucket_key_tuples, s3_source_kwargs

    def select(self, prefixes, thread_count=150,
            count=False, limit=0, output_fields=None, where=None):
        """
        Uses S3 Select to retrieve data
        https://github.com/usdot-its-jpo-data-portal/s3select

        Parameters:
            prefixes: space delimited S3 prefix(es) beneath which all files
                are queried. Each prefix should be in the format of
                "s3://someBucket/someKey".
            thread_count: How many threads to use when executing s3_select api
                        requests. Default of 150 seems to be on safe side. If
                        you increase this there is a chance you'll need also
                        to increase nr of open files on your OS
            count: Boolean field indicating whether or not you want to retrieve the records or just a count of the records.
            limit: What fields or columns to output. Supply a comma
                        delimited string of the field names, assuming that the
                        record is 's'. For example, if you want to retrieve
                        fields 'metadata' and 'payload.coreData' only, supply
                        's.metadata,s.payload.coreData'. Default: all fields
                        will be returned.
            where: WHERE part of the SQL query. Assume that the record is
                        's'. For example, a query could look like this:
                        s.metadata.bsmSource='RV' and
                        s.payload.data.coreData.speed < 15. Default: not
                        supply a WHERE clause.
        Returns:
            JSON object generator if count=False
            Count result generator if count=True
        """
        self.info = []
        command_params = {
            'prefixes': prefixes,
            'verbose': '',
            'thread_count': '',
            'count': '',
            'limit': '',
            'output_fields': '',
            'where': '',
            'profile': '',
            'queue_timeout': ''
        }
        if self.aws_profile and self.aws_profile !='default':
            command_params['profile'] = '--profile {} '.format(self.aws_profile)
        if self.verbose:
            command_params['verbose'] = '-v '
        if thread_count:
            command_params['thread_count'] = '-t {} '.format(thread_count)
        if count:
            command_params['count'] = '-c '
        if limit:
            command_params['limit'] = '-l {} '.format(limit)
        if output_fields:
            command_params['output_fields'] = '-o "{}" '.format(output_fields)
        if where:
            command_params['where'] = '-w "{}" '.format(where)

        s3select_path = os.path.join(os.path.split(os.path.abspath(__file__))[0], 's3select', 's3select.py')
        command = 'python {} {where}{limit}{verbose}{count}{output_fields}{thread_count}{profile}{prefixes}'.format(s3select_path, **command_params)
        self.print_func(command)
        process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        while True:
            line = process.stdout.readline().rstrip()
            if not line:
                last_err = None
                while True:
                    stderr = process.stderr.readline()
                    if stderr:
                        if stderr[:3] != b'\x1b[K':
                            print(stderr.decode('utf-8').strip('\n'))
                        else:
                            last_err = stderr
                    else:
                        break
                break
            if not count:
                yield json.loads(line)
            else:
                yield line

    def get_data_stream(self, bucket, key):
        """
        Get data stream.

        Parameters:
            bucket: name of S3 bucket
            path: key of S3 path

        Returns:
            "Readable" file datastream objects
        """
        obj = self.client.get_object(Bucket=bucket, Key=key)
        if key[-3:] == '.gz':
            gzipped = GzipFile(None, 'rb', fileobj=obj['Body'])
            data = TextIOWrapper(gzipped)
        else:
            data = obj['Body']._raw_stream
        return data

    def newline_json_rec_generator(self, data_stream):
        """
        Receives a data stream that is assumed to be in the newline JSON format
        (one stringified json per line), reads and returns these records as
        dictionary objects one at a time.

        Parameters:
            data_stream: "Readable" file datastream objects

        Returns:
            Iterable array of dictionary objects
        """
        line = data_stream.readline()
        while line:
            if type(line) == bytes:
                line_stripped = line.strip(b'\n')
            else:
                line_stripped = line.strip('\n')

            try:
                if line_stripped:
                    yield json.loads(line_stripped)
            except:
                self.print_func(traceback.format_exc())
                self.print_func('Invalid json line. Skipping: {}'.format(line))
                self.err_lines.append(line)
            line = data_stream.readline()

    def nycdot_rec_generator(self, data_stream):
        """
        Receives a data stream that is assumed to be in the NYCDOT CVP batch JSON format
        (prettified json concatenated), reads and returns these records as
        dictionary objects one at a time.

        Parameters:
            data_stream: "Readable" file datastream objects

        Returns:
            Iterable array of dictionary objects
        """
        line = data_stream.readline()
        temp_string = ''
        while line:
            if type(line) == bytes:
                line_stripped = line.strip(b'\n').decode('utf-8')
            else:
                line_stripped = line.strip('\n')

            if line_stripped == '}{':
                temp_string += '}'
                rec = json.loads(temp_string)
                temp_string = '{'
                yield rec
            else:
                temp_string += line_stripped
            line = data_stream.readline()
        if temp_string:
            rec = json.loads(temp_string)
            yield rec

    def write_recs(self, recs, bucket, key):
        """
        Writes the array of dictionary objects as newline json text file to the
        specified S3 key in the specified S3 bucket

        Parameters:
            recs: array of dictionary objects
            bucket: name of S3 bucket
            path: key of S3 path

        Returns:
            None
        """
        outbytes = "\n".join([json.dumps(i) for i in recs if i]).encode('utf-8')
        self.client.put_object(Bucket=bucket, Key=key, Body=outbytes)

    def write_bytes(self, outbytes, bucket, key):
        """
        Writes the bytes to the specified S3 key in the specified S3 bucket

        Parameters:
            outbytes: bytes
            bucket: name of S3 bucket
            path: key of S3 path

        Returns:
            None
        """
        if type(outbytes) != bytes:
            outbytes = outbytes.encode('utf-8')
        self.client.put_object(Bucket=bucket, Key=key, Body=outbytes)

    def delete_file(self, bucket, key):
        self.client.delete_object(Bucket=bucket, Key=key)

    def move_file(self, source_bucket, source_key, target_bucket, target_key=None):
        source_path = os.path.join(source_bucket, source_key)
        self.print_func('Triggered by file: {}'.format(source_path))

        data_stream = self.get_data_stream(source_bucket, source_key)
        recs = []
        for rec in self.newline_json_rec_generator(data_stream):
            recs.append(rec)

        if recs:
            target_key = source_key if target_key is None else target_key
            target_path = os.path.join(target_bucket, target_key)
            self.print_func('Writing {} records from {} -> {}'.format(len(recs), source_path, target_path))
            self.write_recs(recs, target_bucket, target_key)
        else:
            self.print_func('File is empty: {}'.format(source_path))

        self.print_func('Delete file: {}'.format(source_path))
        self.delete_file(source_bucket, source_key)


class CvPilotFileMover(S3Helper):

    def __init__(self, target_bucket, source_bucket_prefix='usdot-its-datahub-', source_key_prefix=None, validation_queue_names=[], *args, **kwargs):
        super(CvPilotFileMover, self).__init__(*args, **kwargs)
        self.source_bucket_prefix = source_bucket_prefix
        self.source_key_prefix = source_key_prefix or ''
        self.queues = []
        self.pilot_name = None
        self.message_type = None
        self.target_bucket = target_bucket

        if validation_queue_names:
            for validation_queue_name in validation_queue_names:
                sqs = boto3.resource('sqs')
                queue = sqs.get_queue_by_name(QueueName=validation_queue_name)
                self.queues.append(queue)

    def move_file(self, source_bucket, source_key):
        # read triggering file
        source_path = os.path.join(source_bucket, source_key)
        self.print_func('Triggered by file: {}'.format(source_path))

        data_stream = self.get_data_stream(source_bucket, source_key)
        
        # sort all files by generatedAt timestamp ymdh
        ymdh_data_dict = {}

        if 'nycdot-ingest' in source_bucket:
            rec_generator = self.nycdot_rec_generator
        else:
            rec_generator = self.newline_json_rec_generator
    
        for rec in rec_generator(data_stream):
            recordGeneratedAt_ymdh = self.get_ymdh(rec)
            if recordGeneratedAt_ymdh not in ymdh_data_dict:
                ymdh_data_dict[recordGeneratedAt_ymdh] = []
            ymdh_data_dict[recordGeneratedAt_ymdh].append(rec)

        # generate output path
        outfp_func = self.generate_outfp(ymdh_data_dict, source_bucket, source_key)
        if outfp_func is None:
            return

        for ymdh, recs in ymdh_data_dict.items():
            target_key = outfp_func(ymdh)
            target_path = os.path.join(self.target_bucket, target_key)

            # copy data
            self.print_func('Writing {} records from \n{} -> \n{}'.format(len(recs), source_path, target_path))
            self.write_recs(recs, self.target_bucket, target_key)
            self.print_func('File written')
            if self.queues:
                for queue in self.queues:
                    msg = {
                    'bucket': self.target_bucket,
                    'key': target_key,
                    'pilot_name': self.pilot_name,
                    'message_type': self.message_type.lower()
                    }
                    queue.send_message(MessageBody=json.dumps(msg))

        if len(self.err_lines) > 0:
            self.print_func('{} lines not read in file. Keep file at: {}'.format(len(self.err_lines), source_path))
        else:
            self.print_func('Delete file: {}'.format(source_path))
            self.delete_file(source_bucket, source_key)
        return

    def get_ymdh(self, rec):
        # nycdot version
        time_bin = rec.get('eventHeader', {}).get('eventTimeBin', '').replace('/', '')
        if time_bin:
            event_time_bin_array = time_bin.split('-')
            if len(event_time_bin_array[0]) == 2:
                event_time_bin_array[0] = '20'+event_time_bin_array[0]
            recordGeneratedAt_ymdh = '-'.join(event_time_bin_array)
            return recordGeneratedAt_ymdh
        # other cvp version
        recordGeneratedAt = rec['metadata'].get('recordGeneratedAt')
        if not recordGeneratedAt:
            recordGeneratedAt = rec['payload']['data']['timeStamp']
        try:
            dt = datetime.strptime(recordGeneratedAt[:14].replace('T', ' '), '%Y-%m-%d %H:')
        except:
            self.print_func(traceback.format_exc())
            recordReceivedAt = rec['metadata'].get('odeReceivedAt')
            dt = datetime.strptime(recordReceivedAt[:14].replace('T', ' '), '%Y-%m-%d %H:')
            self.print_func('Unable to parse {} timestamp. Using odeReceivedAt timestamp of {}'.format(recordGeneratedAt, recordReceivedAt))
        recordGeneratedAt_ymdh = datetime.strftime(dt, '%Y-%m-%d-%H')
        return recordGeneratedAt_ymdh

    def generate_outfp(self, ymdh_data_dict, source_bucket, source_key):
        if not ymdh_data_dict:
            self.print_func('File is empty: s3://{}/{}'.format(source_bucket, source_key))
            return None
        
        regex_str = r'(?:test-)?{}(.*)-ingest'.format(self.source_bucket_prefix)
        regex_finds = re.findall(regex_str, source_bucket)
        if len(regex_finds) == 0:
            # if source bucket is sandbox
            pilot_name = source_key.split('/')[0]
            message_type = source_key.split('/')[1]
            stream_version = '0'

            original_ymdh = "-".join(source_key.split('/')[-5:-1])
            no_change = "".join(ymdh_data_dict.keys()) == original_ymdh

            if no_change and source_bucket == self.target_bucket:
                self.print_func('No need to reorder data at s3://{}/{}'.format(source_bucket, source_key))
                return None
        else:
            # if source bucket is ingest bucket
            pilot_name = regex_finds[0].lower()
            if pilot_name == 'nycdot':
                message_type = 'EVENT'
            else:
                message_type = source_key.strip(self.source_key_prefix).split('/')[0]

            # get stream version
            filename_prefix = self.target_bucket.replace('-public-data', '')
            regex_str2 = filename_prefix+r'-(?:.*)-public-(\d)-(?:.*)'
            stream_version_res = re.findall(regex_str2, source_key)
            if not stream_version_res:
                stream_version = '0'
            else:
                stream_version = stream_version_res[0]

        def outfp_func(ymdh):
            y,m,d,h = ymdh.split('-')
            ymdhms = '{}-00-00'.format(ymdh)
            uuid4 = str(uuid.uuid4())

            target_filename = '-'.join([filename_prefix, message_type.lower(), 'public', str(stream_version), ymdhms, uuid4])
            target_prefix = os.path.join(pilot_name, message_type, y, m, d, h)
            target_key = os.path.join(target_prefix, target_filename).replace(".gz", "")
            return target_key

        self.pilot_name  = pilot_name
        self.message_type = message_type

        return outfp_func

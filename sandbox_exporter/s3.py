"""
AWS and AWS S3 Helper functions.

"""
import boto3
import botocore.exceptions
import json
import logging
import traceback
from subprocess import Popen, PIPE


class AWS_helper(object):
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
        self.print_func = print
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
            exit()
        except:
            self.print_func(traceback.format_exc())
            self.print_func('Exiting. Unable to establish AWS session with the following profile name: {}'.format(self.aws_profile))
            exit()
        return session


class S3Helper(AWS_helper):
    """
    Helper class for connecting to and working with AWS S3.

    """
    def __init__(self, **kwargs):
        """
        Initialization function of the S3Helper class.

        """
        super(S3Helper, self).__init__(**kwargs)
        self.client = self._get_client()
        self.info = []

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
            'where': ''
        }
        if self.aws_profile:
            command_params['profile'] = '--profile {}'.format(self.aws_profile)
        if self.verbose:
            command_params['verbose'] = '-v'
        if thread_count:
            command_params['thread_count'] = '-t {}'.format(thread_count)
        if count:
            command_params['count'] = '-c'
        if limit:
            command_params['limit'] = '-l {}'.format(limit)
        if output_fields:
            command_params['output_fields'] = '-o "{}"'.format(output_fields)
        if where:
            command_params['where'] = '-w "{}"'.format(where)

        command = 's3select {where} {limit} {verbose} {count} {output_fields} {thread_count} {profile} {prefixes}'.format(**command_params)
        # self.print_func(command)
        process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        while True:
            line = process.stdout.readline().rstrip()
            if not line:
                stderr = process.stderr.read().splitlines()
                if len(stderr) > 4:
                    self.info = [i.decode('utf-8').replace('\x1b[K', '') for i in stderr[-5:]]
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

#!/usr/bin/env python

from __future__ import print_function

import argparse
import boto3
from six.moves.urllib import parse
from six.moves import queue
import threading
import sys
import time
import _queue

_sentinel = object()
max_result_limit_reached = False
total_files = 0
clear_line = '\r\033[K'


class S3ListThread(threading.Thread):
    def __init__(self, s3_prefixes, files_queue, s3):

        threading.Thread.__init__(self)
        self.s3_prefixes = s3_prefixes
        self.files_queue = files_queue
        self.handled = False
        self.s3 = s3

    def run(self):
        for prefix in self.s3_prefixes:
            url_parse = parse.urlparse(prefix)
            bucket = url_parse.netloc
            key_prefix = url_parse.path[1:]

            global total_files
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=bucket,
                Prefix=key_prefix)

            for page in pages:
                if page['KeyCount'] == 0:
                    # no objects returned in the listing
                    break

                if max_result_limit_reached:
                    # limit reached. No more list results needed
                    self.handled = True
                    self.files_queue.put((_sentinel, None))
                    return

                if 'Contents' not in page:
                    continue

                for obj in page['Contents']:
                    # skip 0 bytes files as boto3 deserializer will throw
                    # exceptions for them and anyway there isn't anything useful
                    # in them
                    if obj['Size'] == 0:
                        continue
                    self.files_queue.put((bucket, obj['Key']))
                    total_files = total_files + 1

        self.handled = True
        self.files_queue.put((_sentinel, None))


class ScanOneKey(threading.Thread):
    def __init__(
            self, files_queue, events_queue, s3, output_fields=None, count=None,
            field_delimiter=None, record_delimiter=None, where=None, limit=None,
            max_retries=None):
        threading.Thread.__init__(self)
        self.max_retries = max_retries
        self.limit = limit
        self.where = where
        self.field_delimiter = field_delimiter
        self.record_delimiter = record_delimiter
        self.count = count
        self.output_fields = output_fields
        self.files_queue = files_queue
        self.events_queue = events_queue
        self.handled = False
        self.s3 = s3

    def run(self):
        while True:
            bucket, s3_key = self.files_queue.get()
            s3_path = "s3://{}/{}".format(bucket, s3_key)

            if max_result_limit_reached:
                self.handled = True
                # always add empty message to prevent queue.get from blocking
                # indefinitely
                self.events_queue.put(S3SelectEventResult())
                return
            if bucket is _sentinel:
                # put it back so that other consumers see it
                self.files_queue.put((_sentinel, None))
                self.handled = True
                self.events_queue.put(S3SelectEventResult())
                return
            input_ser = {'JSON': {"Type": "Document"}}
            output_ser = {'JSON': {}}
            if self.field_delimiter is not None or \
                    self.record_delimiter is not None:

                if self.field_delimiter is None:
                    self.field_delimiter = ","
                if self.record_delimiter is None:
                    self.record_delimiter = "\n"

                input_ser = {
                    'CSV':
                        {
                            "FieldDelimiter": self.field_delimiter,
                            "FileHeaderInfo": "NONE",
                            "RecordDelimiter": self.record_delimiter,
                            "QuoteCharacter": ''
                        }
                }
                output_ser = {'CSV': {"FieldDelimiter": self.field_delimiter}}

            if self.count:
                # no need to parse JSON if we are only expecting the count of
                # rows
                output_ser = {'CSV': {"FieldDelimiter": " "}}

            query = "SELECT "
            if self.count:
                query += "count(*) "
            elif self.output_fields is not None:
                query += self.output_fields + " "
            else:
                query += "* "

            query += "FROM s3object s "

            if self.where is not None:
                query += "WHERE " + self.where

            if self.limit > 0:
                query += " LIMIT " + str(self.limit)

            if '.gz' == s3_key.lower()[-3:]:
                input_ser['CompressionType'] = 'GZIP'

            current_try = 0
            while True:
                try:
                    response = self.s3.select_object_content(
                        Bucket=bucket,
                        Key=s3_key,
                        ExpressionType='SQL',
                        Expression=query,
                        InputSerialization=input_ser,
                        OutputSerialization=output_ser,
                    )
                    break
                except Exception as e:
                    self.events_queue.put(S3SelectEventResult(
                        exception=e,
                        max_retries_reached=current_try >= self.max_retries,
                        s3_path=s3_path))
                    time.sleep(0.4)
                    current_try = current_try + 1

            payload_from_previous_event = ""
            end_event_received = False
            for event in response['Payload']:
                if max_result_limit_reached:
                    self.handled = True
                    self.events_queue.put(
                        S3SelectEventResult())
                    return

                if 'Records' in event:
                    records = payload_from_previous_event + \
                              event['Records']['Payload'].decode('utf-8')
                    split_records = records.split("\n")
                    # last "record" is either "\n" or partial record
                    payload_from_previous_event = split_records[-1]
                    self.events_queue.put(
                        S3SelectEventResult(
                            records=split_records[:-1], s3_path=s3_path))
                elif 'Stats' in event:
                    self.events_queue.put(
                        S3SelectEventResult(
                            bytes_returned=
                            event['Stats']['Details']['BytesReturned'],
                            bytes_scanned=
                            event['Stats']['Details']['BytesScanned']))
                elif 'End' in event:
                    end_event_received = True

            if end_event_received:
                self.events_queue.put(S3SelectEventResult(files_processed=1))
            else:
                self.events_queue.put(S3SelectEventResult(
                    exception=Exception(
                        "End event not received data is corrupted. Please "
                        "retry"),
                    max_retries_reached=True,
                    s3_path=s3_path))


class S3SelectEventResult:
    def __init__(self, bytes_returned=0, bytes_scanned=0, files_processed=0,
                 records=(), exception=None, max_retries_reached=False,
                 s3_path=None):
        self.bytes_returned = bytes_returned
        self.bytes_scanned = bytes_scanned
        self.files_processed = files_processed
        self.records = records
        self.exception = exception
        self.max_retries_reached = max_retries_reached
        self.s3_path = s3_path


def format_bytes(bytes_count):
    if bytes_count < 10 ** 3:
        return str(bytes_count) + " B"
    elif bytes_count < 10 ** 6:
        return str(bytes_count // 10 ** 3) + " KB"
    elif bytes_count < 10 ** 9:
        return str(bytes_count // 10 ** 6) + " MB"
    else:
        return str(bytes_count // 10 ** 9) + " GB"


def refresh_status_bar(
        files_processed, records_matched, bytes_scanned, verbose):
    if verbose:
        print('{}Files processed: {}/{}  Records matched: {}  Bytes scanned: {}'
              .format(clear_line, files_processed, total_files, records_matched,
                      format_bytes(bytes_scanned)),
              file=sys.stderr, end="")


def select(prefixes=None, verbose=False, profile=None, thread_count=150,
           count=False, limit=0, output_fields=None, field_delimiter=None,
           record_delimiter=None, where=None, max_retries=20,
           with_filename=False, estimate_cost=False, queue_timeout=10):

    if prefixes is None:
        raise Exception("S3 path prefix must be defined")

    # shortcut as specifying \t from command line is bit tricky with all escapes
    if field_delimiter is not None and "\\t" in field_delimiter:
        field_delimiter = '\t'

    if profile is not None:
        boto3.setup_default_session(profile_name=profile)

    s3 = boto3.client('s3')

    global max_result_limit_reached

    files_queue = queue.Queue(20000)
    events_queue = queue.Queue(20000)

    threads = []
    for x in range(0, thread_count):
        if x == 0:
            # we need only one listing thread
            thread = S3ListThread(prefixes, files_queue, s3)
        else:
            thread = ScanOneKey(
                files_queue, events_queue, s3, count=count, limit=limit,
                output_fields=output_fields, field_delimiter=field_delimiter,
                record_delimiter=record_delimiter, where=where,
                max_retries=max_retries)

        # daemon threads allow for fast exit if max number of records has been
        # specified
        thread.daemon = True
        thread.start()
        threads.append(thread)

    bytes_returned = 0
    bytes_scanned = 0
    files_processed = 0
    records_matched = 0

    while True:
        threads = [t for t in threads if not t.handled]

        if len(threads) == 0 and events_queue.qsize() == 0:
            break

        if max_result_limit_reached:
            break
        try:
            event = events_queue.get(timeout=queue_timeout)
        except _queue.Empty:
            return

        matched_s3_path = event.s3_path

        if event.exception is not None:
            if event.max_retries_reached:
                raise event.exception
            elif verbose:
                print("{}Exception caught while processing {} (will retry). "
                      "Exception: {}"
                      .format(clear_line, event.s3_path, str(event.exception)),
                      file=sys.stderr)

        bytes_returned = bytes_returned + event.bytes_returned
        bytes_scanned = bytes_scanned + event.bytes_scanned
        files_processed = files_processed + event.files_processed

        refresh_status_bar(
            files_processed, records_matched, bytes_scanned, verbose)

        out, err = [], []
        for record in event.records:
            if count:
                records_matched = records_matched + int(record)
            else:
                records_matched = records_matched + 1
                if verbose:
                    print(clear_line, file=sys.stderr, end="")
                if with_filename:
                    print(matched_s3_path + "\t" + record)
                else:
                    print(record)

                refresh_status_bar(
                    files_processed, records_matched, bytes_scanned, verbose)
                if 0 < limit <= records_matched:
                    max_result_limit_reached = True
                    break

    if count:
        if verbose:
            print(clear_line, file=sys.stderr, end="")

        print(records_matched)

    if verbose:
        price_for_bytes_scanned = 0.002 * bytes_scanned / (
                1024 ** 3)
        price_for_bytes_returned = 0.0007 * bytes_returned / (1024 ** 3)
        price_for_requests = 0.0004 * total_files / 1000

        refresh_status_bar(
            files_processed, records_matched, bytes_scanned, verbose)
        if estimate_cost:
            print("\nCost for data scanned: ${0:.2f}"
                  .format(price_for_bytes_scanned), file=sys.stderr)
            print("Cost for data returned: ${0:.2f}"
                  .format(price_for_bytes_returned), file=sys.stderr)
            print("Cost for SELECT requests: ${0:.2f}"
                  .format(price_for_requests), file=sys.stderr)
            total_cost = price_for_bytes_scanned + price_for_bytes_returned \
                         + price_for_requests
            print("Total cost: ${0:.2f}".format(total_cost), file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='s3select makes s3 select querying API much easier and '
                    'faster'
    )
    parser.add_argument(
        "prefixes",
        nargs='+',
        help="S3 prefix (or more) beneath which all files are queried"
    )
    parser.add_argument(
        "-w",
        "--where",
        help="WHERE part of the SQL query"
    )
    parser.add_argument(
        "-d",
        "--field_delimiter",
        help="Field delimiter to be used for CSV files. If specified CSV "
             "parsing will be used. By default we expect JSON input"
    )
    parser.add_argument(
        "-D",
        "--record_delimiter",
        help="Record delimiter to be used for CSV files. If specified CSV "
             "parsing will be used. By default we expect JSON input"
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=0,
        help="Maximum number of results to return"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action='store_true',
        help="Be more verbose"
    )
    parser.add_argument(
        "-c",
        "--count",
        action='store_true',
        help="Only count records without printing them to stdout"
    )
    parser.add_argument(
        "-H",
        "--with_filename",
        action='store_true',
        help="Output s3 path of a filename that contained the match"
    )
    parser.add_argument(
        "-o",
        "--output_fields",
        help="What fields or columns to output"
    )
    parser.add_argument(
        "-t",
        "--thread_count",
        type=int,
        default=150,
        help="How many threads to use when executing s3_select api  requests. "
             "Default of 150 seems to be on safe side. If you increase this "
             "there is a chance you'll need also to increase nr of open files "
             "on your OS"
    )
    parser.add_argument(
        "--profile",
        help="Use a specific AWS profile from your credential file."
    )
    parser.add_argument(
        "-M",
        "--max_retries",
        type=int,
        default=20,
        help="Maximum number of retries per queried S3 object in case API "
             "request fails"
    )

    parser.add_argument(
        "-e",
        "--estimate_cost",
        action='store_true',
        help="Provide cost estimate associated with query. Only works with verbose "
            "flag on."
    )

    parser.add_argument(
        "-T",
        "--queue_timeout",
        type=int,
        default=10,
        help="Change timeout value for queue (in seconds). Default: 10."
    )

    args = parser.parse_args()
    select(**vars(args))

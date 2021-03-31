# sandbox_exporter
Package to load, query, and export data from ITS DataHub's sandbox efficiently.
For more information on ITS Sandbox data, please refer to the [ITS Sandbox README page](https://github.com/usdot-its-jpo-data-portal/sandbox/tree/split-repo#exporting-data-to-csv-with-sandbox-exporter).

This repository currently includes several importable utilities for interacting with the ITS Sandbox S3 buckets. These utilities uses Python 3.x as the primary programming language and can be executed across operative systems.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for use, development, and testing purposes.

### Prerequisites

1. Have access to [Python 3.6+](https://www.python.org/download/releases/3.0/). You can check your python version by entering `python --version` and `python3 --version` in command line.
2. Have access to the command line of a machine. If you're using a Mac, the command line can be accessed via the [Terminal](https://support.apple.com/guide/terminal/welcome/mac), which comes with Mac OS. If you're using a PC, the command line can be accessed via the Command Prompt, which comes with Windows, or via [Cygwin64](https://www.cygwin.com/), a suite of open source tools that allow you to run something similar to Linux on Windows.
3. Have your own Free Amazon Web Services account.
	- Create one at http://aws.amazon.com
4.  Obtain Access Keys:
	- On your Amazon account, go to your profile (at the top right)
	- My Security Credentials > Access Keys > Create New Access Key
	- Record the Access Key ID and Secret Access Key ID (you will need them in step 4)
5. Save your AWS credentials in your local machine, using one of the following method:
	- shared credentials file: instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file.
	- environmental variables: instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#environment-variables

### Installation

1. Download the script by cloning the module's [code repository on GitHub](https://github.com/usdot-its-jpo-data-portal/wzdx_sandbox). You can do so by running one of the following in command line. If unfamiliar with how to clone a repository, follow the [official GitHub guide](https://help.github.com/en/articles/cloning-a-repository).
    - via HTTP: `git clone https://github.com/usdot-its-jpo-data-portal/sandbox_exporter.git`
    - via SSH (if using 2-factor authentication): `git clone git@github.com:usdot-its-jpo-data-portal/sandbox_exporter.git`
2. Navigate into the repository folder by entering `cd sandbox_exporter` in command line.
3. Run `pip install -e .` to install the sandbox_exporter Python package.
4. Install the required packages by running `pip install -r requirements.txt`.

### Usage

#### Use as python package
`SandboxExporter` can be imported into your own code by adding the following statement:

`from sandbox_exporter.exporter import SandboxExporter`

Sample usage have been provided in the [demo.ipynb](demo.ipynb) file in this repository.

#### Use as command line script
The Sandbox Exporter can also be used in the command line directly.

Run `python sandbox_exporter/exporter.py --help` in commandline to get the script usage information below:
```
usage: exporter.py [-h] [--bucket BUCKET] [--pilot PILOT]
                   [--message_type MESSAGE_TYPE] --sdate SDATE [--edate EDATE]
                   [--output_convention OUTPUT_CONVENTION] [--json]
                   [--aws_profile AWS_PROFILE] [--zip] [--log] [--verbose]
                   [--limit LIMIT] [--output_fields OUTPUT_FIELDS]
                   [--where WHERE]

Script for exporting ITS sandbox data from specified date range to merged CSV
files or JSON newline files.

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
  --verbose             Supply flag if script progress should be verbose. Cost
                        information associated with your query will be printed
                        if verbose. Default: False
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
```
Example Usage in command line:
- Retrieve all WYDOT BSM data from 2020-01-22:
`python -u sandbox_exporter/exporter.py --pilot wydot --message_type bsm -sdate 2020-01-22`

- Retrieve all WYDOT BSM data between 2020-01-22:
`python -u sandbox_exporter/exporter.py --pilot wydot --message_type bsm --sdate 2020-01-22`

- Retrieve all WYDOT BSM data between 2020-01-22 to 2020-01-24 in json newline format (instead of flattened CSV), only retrieving the metadata field:
`python -u sandbox_exporter/exporter.py --pilot wydot --message_type bsm --sdate 2020-01-22 --edate 2020-01-24 --json --output_fields 's.metadata' --verbose`

## Built With

* [Python 3.6+](https://www.python.org/download/releases/3.0)
* [s3select](https://github.com/usdot-its-jpo-data-portal/s3select): S3select package forked from [Marko Bastovanovic's repository](https://github.com/vast-engineering/s3select). Used to interact with AWS's S3 Select API.
* [requests](https://pypi.org/project/requests/) 
* [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html?id=docs_gateway)

## Contributing

1. [Fork it](https://github.com/usdot-its-jpo-data-portal/sandbox_exporter/fork)
2. Create your feature branch (git checkout -b feature/fooBar)
3. Commit your changes (git commit -am 'Add some fooBar')
4. Push to the branch (git push origin feature/fooBar)
5. Create a new Pull Request

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for general good practices on code of conduct, and the process for submitting pull requests.

## License

This project is licensed under the Apache 2.0 License. - see the [LICENSE](LICENSE) file for details

## Release History
* 0.1.0
  * Initial version

## Known Bugs
*

## Contact information
ITS DataHub Team: data.itsjpo@dot.gov
Distributed under Apache 2.0 License. See *LICENSE* for more information.

## Credits and Acknowledgment
Thank you to the Department of Transportation for funding to develop this project and Marko Bastovanovic for your S3select Python package.

## CODE.GOV Registration Info
* __Agency:__ DOT
* __Short Description:__ Python package for loading, querying, and exporting data from ITS DataHub's sandbox efficiently.
* __Status:__ Beta
* __Tags:__ transportation, connected vehicles, intelligent transportation systems, python, ITS Sandbox, S3
* __Labor Hours:__ 0
* __Contact Name:__ Brian Brotsos
* __Contact Phone:__ 202-366-9013

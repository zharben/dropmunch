# dropmunch - data file processor
# - for any unprocessed data files in the data directory :
# -   confirms that there is a spec for the data file format
# -   validates and processes data based on spec
# -   persists data in the database
# -   cleans up the data file (by ...)
import re
import dataset
import builtins
#import dateutil.parser
import datetime
import fnmatch
import logging
import os
from dropmunch import munch_spec

data_directory = '/data/'

datafile_spec_pattern = '([a-zA-Z0-9]+)'

# This is a very lenient ISO8601 regex pattern -
# allows invalid months and days, hours minutes and seconds.
# a more robust regex is provided here : https://pypi.python.org/pypi/iso8601.py
datafile_timestamp_pattern = '(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)+'

# An extra regex group is appeneded before the ".txt" extension -
# to facilitate file labeling in unit tests.
# This *does not* precisely match the project specifications.
datafile_filename_pattern = datafile_spec_pattern + '_' + datafile_timestamp_pattern + '(.*).txt'

# example timestamp : 2007-10-01T13:47:12.345Z
timestamp_format_pattern = '%Y-%m-%dT%H:%M:%S.%fZ'


class DataFileSpec:
    def __init__(self, spec, timestamp):
        self.spec = spec
        self.timestamp = timestamp


def parse_timestamp(timestamp):
    try:
        # dateutil.parser.parse(timestamp) looks more robust, however it
        # doesn't meet the precise project requirements.
        # Instead, we'll use datetime.strptime in order to specify the format pattern
        return datetime.datetime.strptime(timestamp, timestamp_format_pattern)
    except ValueError:
        logging.getLogger('munch_data_files').error('timestamp {0} doesn\'t match expected format {1}'.format(timestamp, timestamp_format_pattern))
        return None


class MunchData:
    def __init__(self, working_directory=None, log_each_row=False):
        self.working_directory = working_directory if working_directory is not None else os.getcwd() + data_directory
        self.operating_system = os
        self.processed_count = 0
        self.ready_for_processing = 0
        self.file_failure_count = 0
        self.row_failure_count = 0
        self.log_each_row = log_each_row
        self.log = logging.getLogger('MunchData')
        self.db = dataset.connect('postgresql://your_database_user:your_database_password@localhost:15432/dropmunch')
        pass

    def process_data_files(self):
        try:
            for file, datafile_spec, import_log_row in self.get_unprocessed_data_files():
                processed_row_count = self.process_datafile(file,
                                                            datafile_spec,
                                                            import_log_row['id'],
                                                            import_log_row['num_rows_processed'])
                if processed_row_count == 0:
                    self.file_failure_count += 1
                    self.log.warn('No rows were processed from file {0}'.format(file))
                    self.update_import_log(import_log_row['id'], 0, 'failed')
                else:
                    if not self.log_each_row:
                        self.update_import_log(import_log_row['id'], processed_row_count, 'complete')
                    self.processed_count += 1
        except StopIteration:
            pass
        except Exception as e:
            self.log.error('An error occurred while row files. Error : {0}'.format(e))

    def get_unprocessed_data_files(self):
        try:
            for file in fnmatch.filter(self.operating_system.listdir(self.working_directory), "*.txt"):
                if fnmatch.fnmatch(file,'.*'):
                    self.log.info('Ignoring dotfile {0}'.format(file))
                else:
                    datafile_spec = self.get_datafile_spec(file)
                    import_log_row = self.create_import_log(datafile_spec)

                    if datafile_spec is None:
                        self.log.error('Failed to load datafile spec for filename {0}. '
                                       'This file will be skipped'.format(file))
                        self.file_failure_count += 1
                        continue
                    elif import_log_row is None:
                        self.log.error('Failed to load import_log for spec name {0}. '
                                       'This file will be skipped'.format(datafile_spec.spec.name))
                        self.file_failure_count += 1
                        continue
                    else:
                        self.ready_for_processing += 1
                        yield file, datafile_spec, import_log_row
        except StopIteration:
            pass

    def process_datafile(self, file, datafile_spec, import_log_id, skip_rows=0):
        row_count = 1
        processed_row_count = 0

        with open(self.working_directory + file, 'r') as datafile:
            if skip_rows > 0:
                self.log.info('Skipping {0} rows for partially processed data file {1}'.format(skip_rows, file))

            for _ in range(skip_rows):
                row_count += 1
                next(datafile)
            for row in datafile:
                row_count += 1
                row = row.rstrip('\n')
                if not datafile_spec.spec.validate_row(row):
                    self.log.error('Failed to validate row number {0} from {1}'.format(row_count, file))
                    self.row_failure_count += 1
                else:
                    processed_row = dict(import_log_id=import_log_id)
                    for column, spec_column in zip(datafile_spec.spec.split_row(row), datafile_spec.spec.columns):
                        processed_row[spec_column.name] = column

                    if not self.persist_row(datafile_spec, processed_row):
                        self.log.error('Failed to insert row number {0} from {1}'.format(row_count, file))
                        self.row_failure_count += 1
                    else:
                        if self.log_each_row:
                            self.update_import_log(import_log_id, 1)
                        processed_row_count += 1

        return processed_row_count

    def create_import_log(self, datafile_spec):
        try:
            timestamp = self.format_datetime_for_db(datafile_spec.timestamp)
            with self.db as transaction:
                import_format_row = transaction['import_format'].find_one(name=datafile_spec.spec.name)

                if import_format_row is None:
                    self.log.error('Unable to update import_log - '
                                   'import_format wasn\'t found for spec name {0}'.format(datafile_spec.spec.name))
                else:
                    import_log_row = transaction['import_log'].find_one(import_format_id=import_format_row['id'], creation_date=timestamp)

                    if import_log_row is None:
                        return transaction['import_log'].find_one(id=transaction['import_log']
                                                                  .insert(dict(import_format_id=import_format_row['id'],
                                                                  creation_date=timestamp,
                                                                  import_status='inprogress',
                                                                  num_rows_processed=0)))
                    elif import_log_row['import_status'] == 'complete':
                        self.log.error('Found existing import_log for spec name {0} '
                                       'with timestamp={1} and import_status={2}.'
                                       'It appears this file has already been processed! Skipping.'.
                                       format(datafile_spec.spec.name, timestamp, 'complete'))
                        return None
                    else:
                        self.log.info('Found existing import_log for spec name {0} with import_status {1}'.
                                      format(datafile_spec.spec.name, import_log_row['import_status']))
                        return import_log_row
        except Exception as e:
            self.log.error('Failed to create import log for spec name {0}. Error: {1}'.
                           format(datafile_spec.spec.name, e))
            return None

    def format_datetime_for_db(self,datetime):
        return datetime.isoformat()[:-3]

    def update_import_log(self, import_log_id, processed_count, import_status='inprogress'):
        try:
            with self.db as transaction:
                self.log.info('updating import_log id {0} - '
                              'incrementing num_rows_processed by {1} '
                              'and setting status to {2}'.format(import_log_id,
                                                                 processed_count,
                                                                 import_status))
                # TODO hopefully, reading within a transaction obtains a row lock?
                import_log_row = transaction['import_log'].find_one(id=import_log_id)
                import_log_row['num_rows_processed'] = import_log_row['num_rows_processed'] + processed_count
                import_log_row['import_status'] = import_status
                transaction['import_log'].update(import_log_row,['id'])
        except Exception as e:
            self.log.warn('An error occurred while updating import_log for id {0}. '
                          'Error : {1}'.format(import_log_id,e))

    def get_datafile_spec(self, filename):
        match = re.match(datafile_filename_pattern, filename)

        if match:
            spec_name = match.group(1)
            timestamp = match.group(2)
            if not munch_spec.validate_spec_name(spec_name):
                self.log.error('The spec name {0} is not valid. '
                               'File {1} will be skipped'.format(spec_name, filename))
                return None
            elif not parse_timestamp(timestamp):
                self.log.error('The spec timestamp {0} is not valid. '
                               'File {1} will be skipped'.format(timestamp, filename))
                return None
            else:
                munch_spec_instance = munch_spec.MunchSpec(None)
                spec = munch_spec_instance.load_spec_from_db(spec_name)

                if spec is not None:
                    return DataFileSpec(spec, parse_timestamp(timestamp))
                else:
                    self.log.warn('No spec was found in the database for name {0}. '
                                  'This file will be skipped'.format(spec_name))
                    return None

    def persist_row(self, datafile_spec, row):
        try:
            with self.db as transaction:
                spec_table_name = 'import_data_{0}'.format(datafile_spec.spec.name)
                transaction[spec_table_name].insert(row)
                return True
        except Exception as e:
            self.log.error('An error occurred while persisting row into {0}'.format(spec_table_name), e)
            return False

    def cleanup(self):
        pass

def main():
    pass

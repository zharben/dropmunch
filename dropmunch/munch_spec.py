# dropmunch - spec file processor
# - for any unprocessed spec files in the spec directory :
# -   validates and processes spec data
# -   persists spec in the database
# -   cleans up the spec file (by ...)
import csv
import dataset
import fnmatch
import logging
import os
import six
import sqlalchemy
import re
from timeit import default_timer as timer
from flufl.enum import Enum

spec_directory = '/specs/'

# spec file column names, as specified in the project description
spec_name_key = 'column name'
spec_width_key = 'width'
spec_datatype_key = 'datatype'

spec_fields = [spec_name_key, spec_width_key, spec_datatype_key]

class SpecColumn:
    def __init__(self, name, width, datatype, nullable=False):
        self.name = name
        self.datatype = datatype
        self.width = width
        self.nullable = nullable

    def validate_column(self, column):
        spec_datatype = SpecDataType[self.datatype];

        if spec_datatype == SpecDataType.TEXT:
            return isinstance(column, six.string_types)
        elif spec_datatype == SpecDataType.INTEGER:
            return is_integer(column)
        elif spec_datatype == SpecDataType.BOOLEAN:
            return column.strip() in ['0', '1']

        raise ValueError('datatype {0} is not implemented'.format(spec_datatype))


class Spec:
    def __init__(self, name, columns=None):
        self.name = name
        self.columns = columns if columns is not None else []

        self.total_col_width = 0

        if columns is not None:
            for column in columns:
                self.total_col_width += column.width

    def add_column(self, column):
        self.columns.append(column)

    def validate_row(self, unprocessed_row):
        if not isinstance(unprocessed_row, six.string_types):
            logging.getLogger('munch_spec').error('Error validating row - unable to parse string!')
            return False
        elif len(unprocessed_row) != self.total_col_width:
            logging.getLogger('munch_spec').error('Error validating row - '
                                                  'expected width is {0}, but row contains {1} columns'
                                                  .format(self.total_col_width, len(unprocessed_row)))
            return False
        else:
            for column, spec_column in zip(self.split_row(unprocessed_row), self.columns):
                if not spec_column.validate_column(column):
                    logging.getLogger('munch_spec').error('Error validating row - '
                                                          'column {0} did not match spec column {1}'
                                                          .format(column, spec_column.name))
                    return False

        return True

    def split_row(self, row):
        columns = []
        index = 0
        for spec_column in self.columns:
            sliceend = index+spec_column.width
            columns.append(row[index:sliceend])
            index = sliceend

        return columns


class SpecDataType(Enum):
    TEXT='TEXT'
    BOOLEAN='BOOLEAN'
    INTEGER='INTEGER'


def elapsed(timer, start):
    return round((timer() - start) * 1000,0)


class MunchSpec:
    def __init__(self, working_directory=None):
        self.working_directory = working_directory if working_directory is not None else os.getcwd() + spec_directory
        self.operating_system = os
        self.ready_to_process_count = 0
        self.processed_count = 0
        self.log = logging.getLogger('MunchSpecs')

        try:
            self.db = dataset.connect('postgresql://your_database_user:your_database_password@localhost:15432/dropmunch')
        except Exception as e:
            self.log.fatal('Error : {0}'.format(e))
            raise SystemExit('Failed to connect to the database - aborting.')

    def process_spec_files(self):
        start = timer()
        failed_count = 0
        try:
            for file in self.get_unprocessed_spec_files():
                if not self.process_spec_from_file(file):
                    self.log.error('Failed to process spec from file {0}'.format(file))
                    failed_count += 1
        except Exception as e:
            self.log.error('An error occurred while processing spec files. Error : {0}'.format(e))
        finally:
            self.log.info('Completed processing {0} spec files, with {1} failures in {2} ms'
                           .format(self.processed_count, failed_count, elapsed(timer, start)))

    def get_unprocessed_spec_files(self):
        start = timer()
        try:
            for file in fnmatch.filter(self.operating_system.listdir(self.working_directory), "*.csv"):
                if fnmatch.fnmatch(file,'.*'):
                    self.log.info('Ignoring dotfile {0} - already processed'.format(file))
                else:
                    self.ready_to_process_count += 1
                    yield file
        except StopIteration:
            pass
        finally:
            self.log.info('Completed retrieval of {0} unprocessed files in {1} ms'
                           .format(self.ready_to_process_count, elapsed(timer, start)))

    def process_spec_from_file(self, file):
        start = timer()
        line_count = 1
        try:
            with open(self.working_directory + file, 'r') as csvfile:
                spec = Spec(file.rstrip('.csv'))
                reader = csv.DictReader(csvfile)

                if reader.fieldnames is None or set(reader.fieldnames) != set(spec_fields):
                    self.log.error('Spec file {0} is missing the header row'.format(file))
                    return False

                for row in reader:
                    line_count += 1
                    if len(row) > len(spec_fields):
                        self.log.error('Spec file row {0} has too many columns'.format(line_count))
                        return False
                    else:
                        spec_column = self.init_spec_column(row, line_count)
                        if spec_column:
                            spec.add_column(spec_column)
                        else:
                            return False

            if line_count <= 1:
                self.log.error('Spec file {0} is empty'.format(file))
                return False
            else:
                if self.persist_spec(spec):
                    self.processed_count += 1
                    return True
                else:
                    return False
        except Exception as e:
            self.log.error('Failed to process spec from file {0}. Error : {1}'.format(file, e))
            return False
        finally:
            self.log.info('Completed retrieval of {0} unprocessed files in {1} ms'
                           .format(self.processed_count, elapsed(timer, start)))

    def init_spec_column(self, attributes, row_number, spec_name_key_override=spec_name_key):
        """Spec_name_key_override is present to support differences
           between specs loaded from files vs database.

           Files use 'column name', whereas the db uses 'name'"""

        name = attributes[spec_name_key_override]
        width = attributes[spec_width_key]
        datatype = attributes[spec_datatype_key]

        if None in [name, width, datatype]:
            self.log.error('Spec file row {0} is missing one or more columns'.format(row_number))
            return False

        try:
            if SpecDataType(datatype) is None:
                self.log.error('Spec file row {0} has invalid datatype attribute {1}'.format(row_number, datatype))
                return False
        except:
            # combining "if SpecDataType(...) is None" and exception handling -
            # having difficulty making enumerations work as expected!
            self.log.error('Spec file row {0} has invalid datatype attribute {1}'.format(row_number, datatype))
            return False

        if not validate_spec_name(name):
            self.log.error('Spec file row {0} has invalid name attribute'.format(row_number))
            return False

        if not is_integer(width):
            self.log.error('Spec file row {0} has invalid width attribute {1}'.format(row_number, width))
            return False

        if float(width) <= 0:
            self.log.error('Spec file row {0} has negative width attribute {1}'.format(row_number, width))
            return False

        return SpecColumn(name, width, datatype)

    def delete_all_specs(self):
        start = timer()
        deleted = 0
        try:
            # "with" handles commit / rollback
            with self.db as transaction:
                for import_format_row in transaction['import_format']:
                    # each invocation of delete_spec creates a separate transaction
                    # dataset library claims to allow nested transactions, but I'm
                    # unsure what that means in practice.
                    self.delete_spec(import_format_row['id'], transaction)
                    deleted += 1
        except Exception as e:
            self.log.error('An error occurred while deleting all specs from '
                           'import_format/import_format_column. Error : {0}'.format(e))
        finally:
            self.log.info('Completed deletion of {0} unprocessed files in {1} ms'
                           .format(deleted, elapsed(timer, start)))

    def delete_spec(self, import_format_id, transaction=None):
        transaction = transaction if transaction is not None else self.db
        try:
            # "with" handles commit / rollback
            with transaction:
                import_format = transaction['import_format']
                import_log = transaction['import_log']
                import_format_column = transaction['import_format_column']

                # Get a specific user
                import_format_row = import_format.find_one(id=import_format_id)
                if import_format_row is None or len(list(import_format_row)) == 0:
                    self.log.error('No row was found in import_format for spec id {0}'.format(import_format_id))
                    return False
                else:
                    spec_data_table = 'import_data_{0}'.format(import_format_row['name'])
                    self.log.info('Deleting table {0}'.format(spec_data_table))
                    transaction[spec_data_table].drop()

                    self.log.info('Deleting import_format row id {0}, and child rows in import_format_column'.format(import_format_id))
                    import_format_column.delete(import_format_id=import_format_id)
                    import_log.delete(import_format_id=import_format_id)
                    import_format.delete(id=import_format_id)

                return True

        except sqlalchemy.exc.SQLAlchemyError as e:
            # catch db exceptions - this method is expected to return True/False
            self.log.error('Failed to delete import_format row id {0}. Error : {1}'.format(import_format_id, e))
            return False

    def persist_spec(self, spec):
        start = timer()
        try:
            # "with" handles commit / rollback
            with self.db as transaction:
                import_format = transaction['import_format']

                if import_format.find_one(name=spec.name):
                    self.log.warn('Found existing spec with name {0} in import_format.  '
                                  'Updating existing specs is not supported at this time - skipping processing'.format(spec.name))
                    return True

                import_format_column = transaction['import_format_column']
                format_id = import_format.insert(dict(name=spec.name))

                for column in spec.columns:
                    import_format_column.insert(dict(import_format_id=format_id,
                                                   name=column.name,
                                                   width=column.width,
                                                   datatype=column.datatype,
                                                   nullable=column.nullable))

                self.persist_spec_table(spec)

                return True
        except Exception as e:
        # except sqlalchemy.exc.SQLAlchemyError as e:
            # catch db exceptions - this method is expected to return True/False
            self.log.error('Failed to persist spec with name {0} into import_format. Error : {1}'.format(spec.name, e), )
            return False
        finally:
            self.log.info('Completed persisting spec into import_format in {0} ms'
                           .format(elapsed(timer, start)))

    def get_sql_type(self, datatype):
        try:
            if SpecDataType(datatype) is None:
                raise ValueError('datatype {0} is not implemented'.format(datatype))
                return False
            else:
                if SpecDataType[datatype] == SpecDataType.TEXT:
                    return sqlalchemy.Text
                elif SpecDataType[datatype] == SpecDataType.INTEGER:
                    return sqlalchemy.Integer
                elif SpecDataType[datatype] == SpecDataType.BOOLEAN:
                    return sqlalchemy.Boolean
        except:
            # combining "if SpecDataType(...) is None" and exception handling -
            # having difficulty making enumerations work as expected!
            raise ValueError('datatype {0} is not implemented'.format(datatype))

    def persist_spec_table(self, spec):
        start = timer()
        # TODO - the Boolean return value is not currently used. Remove?
        try:
            # "with" handles commit / rollback
            with self.db as transaction:
                spec_table_name = 'import_data_{0}'.format(spec.name)
                spec_table = None

                try:
                    spec_table = transaction.load_table(spec_table_name)
                except sqlalchemy.exc.SQLAlchemyError as e:
                    self.log.info('Spec table {0} doesn\'t exist yet. We\'ll attempt to create it.'.format(spec_table_name))


                if spec_table is None:
                    spec_table = transaction.create_table(spec_table_name)
                    spec_table.create_column('import_log_id', sqlalchemy.Integer)

                    for column in spec.columns:
                        spec_table.create_column(column.name,self.get_sql_type(column.datatype))

                    # TODO - add foreign key import_log.id => spec_table.import_log_id

        except sqlalchemy.exc.SQLAlchemyError as e:
            # catch db exceptions - this method is expected to return True/False
            self.log.error('Failed to create spec table import_data_{0}. Error : {1}'.format(spec.name, e))
            return False

        else:
            self.log.info('Created spec table import_{0} in {1} ms'.format(spec.name, elapsed(timer, start)))
            return True


    def load_spec_from_db(self, name):
        start = timer()
        try:
            import_format_row = self.db['import_format'].find_one(name=name)

            if import_format_row is None:
                self.log.error('No spec was found in import_format for name {0}'.format(name))
                return None
            else:
                import_format_columns = self.db['import_format_column'].find(import_format_id=import_format_row['id'])

                spec_columns = []

                for import_format_column in import_format_columns:
                    # override 'spec_name_key' with db column name, which is ... 'name'!
                    spec_column = self.init_spec_column(import_format_column, 'n/a', 'name')

                    if spec_column:
                        spec_columns.append(spec_column)
                    else:
                        self.log.error('Failed to initialize spec column from db '
                                       'for name {0}, import_format_id {1}'.format(name, import_format_row['id']))
                        return None

                if len(spec_columns) == 0:
                    self.log.error('No spec columns were found in import_format_column '
                                   'for name {0}, import_format_id {1}'.format(name, import_format_row['id']))
                    return None
                else:
                    return Spec(name, spec_columns)
        except Exception as e:
            self.log.error('An error occurred while loading spec from db (import_format). Error : {0}'.format(e))
            return None
        finally:
            self.log.info('Completed loading spec from db in {0} ms'
                           .format(elapsed(timer, start)))

def validate_spec_name(name):
    try:
        if not isinstance(name, six.string_types):
            logging.getLogger('MunchSpecs').error('Spec name is not a string'.format(name))
        elif not re.match('[a-zA-Z0-9]+', name):
            logging.getLogger('MunchSpecs').error(
                           'Spec column name {0} is invalid. '
                           'Must not be empty, and may only contain uppercase '
                           'or lower-case characters, or numbers'.format(name))
        else:
            return True

    except Exception as e:
        logging.getLogger('MunchSpecs').error('Failed to evaluate spec name {0}. Error : {1}'.format(name, e))

    return False


def is_integer(val):
    try:
        cast = float(val)
        return int(cast) == cast
    except ValueError:
        return False

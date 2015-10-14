import os
import unittest
import dateutil.parser
from dropmunch import munch_data, munch_spec

iso8601_timestamp1 = '2007-10-01T13:47:12.345Z'
valid_datafile_name = 'DATAspecvalid_{0}.txt'.format(iso8601_timestamp1)

class DataFileProcessing(unittest.TestCase):

    def setUp(self):
        self.working_directory = os.getcwd() + '/fixtures/'
        self.munch_data = munch_data.MunchData(self.working_directory)
        self.munch_spec = munch_spec.MunchSpec(self.working_directory)

    def tearDown(self):
        self.munch_spec.delete_all_specs()

    """Test validation and processing of data files"""
    def test_datafile_found_db_spec(self):
        spec_name = 'DATAspecvalid'
        spec_columns = [munch_spec.SpecColumn('color', 7, 'TEXT')]
        spec = munch_spec.Spec(spec_name, spec_columns)

        self.munch_spec.persist_spec(spec)
        datafile_spec = self.munch_data.get_datafile_spec(valid_datafile_name)

        self.assertIsNotNone(datafile_spec,
                             'providing a filename corresponding to a spec available in the db '
                             'produces a valid DatafileSpec object')

    def test_valid_datafile(self):
        spec_name = 'DATAspecvalid'
        spec_columns = [munch_spec.SpecColumn('color', 7, 'TEXT'),
                        munch_spec.SpecColumn('sohot_rightnow', 1, 'BOOLEAN')]
        spec = munch_spec.Spec(spec_name, spec_columns)
        datafile_spec = munch_data.DataFileSpec(spec, dateutil.parser.parse('2007-10-01T13:47:12.345Z'))
        self.munch_spec.persist_spec(spec)
        import_log_row = self.munch_data.create_import_log(datafile_spec)

        datafile_spec = self.munch_data.get_datafile_spec(valid_datafile_name)

        self.assertEquals(self.munch_data.process_datafile(valid_datafile_name, datafile_spec, import_log_row['id']),
                          3,
                          '3 rows are processed from valid datafile')


    def test_padded_boolean_column(self):
        self.munch_spec.process_spec_from_file('DATApaddedbool.csv')
        spec = self.munch_spec.load_spec_from_db('DATApaddedbool')
        datafile_spec = munch_data.DataFileSpec(spec, dateutil.parser.parse(iso8601_timestamp1))
        import_log_row = self.munch_data.create_import_log(datafile_spec)

        self.assertEquals(self.munch_data.process_datafile('DATApaddedbool_{0}.txt'.format(iso8601_timestamp1),
                                                           datafile_spec,
                                                           import_log_row['id']),
                          3,
                          '3 rows are processed from valid datafile')

    def test_invalid_datafile_row(self):
        self.munch_spec.process_spec_from_file('DATAspecvalid.csv')
        spec = self.munch_spec.load_spec_from_db('DATAspecvalid')
        datafile_spec = munch_data.DataFileSpec(spec, dateutil.parser.parse(iso8601_timestamp1))
        import_log_row = self.munch_data.create_import_log(datafile_spec)

        filename = 'DATAspecvalid_{0}_badrow.txt'.format(iso8601_timestamp1)
        processed_row_count = self.munch_data.process_datafile(filename,
                                                               datafile_spec,
                                                               import_log_row['id'])

        self.assertEquals(processed_row_count,
                          2,
                          '2 rows are processed from valid datafile {0}'.format(filename))
        self.assertEquals(self.munch_data.row_failure_count,
                          1,
                          '1 row failed to be processed from datafile {0}'.format(filename))

### TODO - implement unit tests for invalid data conditions :
    # def test_data_file_missing_db_spec_found_file_spec(self):
    #     """when data file's spec is found in filesystem, but
    #     not in the database a warning is logged and the data file is skipped"""
    #     self.assertTrue(False)
    #
    # def test_data_file_missing_spec(self):
    #     """when data file's spec isn't found in database or in the filesystem, an error is raised"""
    #     self.assertTrue(False)
    #
    # def test_data_file_empty(self):
    #     """when a data file is processed containing no rows, an error is raised"""
    #     self.assertTrue(False)
    #
    # def test_data_file_io_error(self):
    #     """when an io error occurs, an error is raised"""
    #     self.assertTrue(False)
    #
    # def test_data_column_invalid_datatype(self):
    #     """when a column can't be parsed using on the spec width and datatype, an error is raised"""
    #     self.assertTrue(False)
    #
    # def test_data_row_missing_characters(self):
    #     """when a row contains fewer characters than spec, an error is raised"""
    #     self.assertTrue(False)
    #
    # def test_data_row_extra_characters(self):
    #     """when a row contains more characters than spec, an error is raised"""
    #     self.assertTrue(False)
    #
    # def test_data_error_saving_to_db(self):
    #     """when an error occurs while saving data to the database, an error is raised"""
    #     self.assertTrue(False)
import os
import unittest
from dropmunch import munch_spec
from unittest.mock import MagicMock


class SpecFileProcessing(unittest.TestCase):
    """Test validation and processing of specification files"""
    def setUp(self):
        self.working_directory = os.getcwd() + '/fixtures/'
        self.munch_spec = munch_spec.MunchSpec(self.working_directory)
        self.munch_spec.operating_system = MagicMock()

    def tearDown(self):
        self.munch_spec.delete_all_specs()

    def test_no_spec_files(self):
        self.munch_spec.operating_system.listdir = MagicMock(return_value=[])
        try:
            next(self.munch_spec.get_unprocessed_spec_files())
        except StopIteration:
            pass
        finally:
            self.assertEqual(self.munch_spec.processed_count, 0,
                             'no spec files are processed')
            self.assertEqual(self.munch_spec.ready_to_process_count, 0,
                             'no spec files are found')

    def test_spec_filename_begins_with_dot(self):
        """when a spec filename starts with a 'dot', it is ignored, and an info level message is logged"""
        self.munch_spec.operating_system.listdir = MagicMock(return_value=['.dotfile.csv'])
        try:
            with self.assertLogs('MunchSpecs', level='INFO') as logged:
                next(self.munch_spec.get_unprocessed_spec_files())
        except StopIteration:
            self.assertRegex(logged.output[0], 'INFO:MunchSpecs:Ignoring.*',
                             'spec files having filename starting with a dot are ignored, '
                             'and produce an info log message')
            pass

    def test_spec_file_empty(self):
        """when a spec file is processed containing no columns, an error level message is logged"""
        filename = 'spec_empty.csv'
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'empty spec file is not processed successfully')
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec file {0} is missing the header row'.format(filename),
                             'empty spec file produces an error log message')

    def test_spec_no_header_row(self):
        """when the spec file's first row isn't formatted as a header, an error is raised"""
        filename = 'spec_no_header.csv'
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'spec file missing header row is not processed successfully')
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec file {0} is missing the header row'.format(filename),
                             'spec file missing header row produces an error log message')

    def test_spec_lt_three_columns(self):
        """when a row contains fewer than three columns, an error is raised"""
        filename = 'spec_two_columns.csv'
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'spec file missing columns is not processed successfully')
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec file row 2 is missing one or more columns',
                             'spec file missing columns produces an error log message')

    def test_spec_gt_three_columns(self):
        filename = 'spec_four_columns.csv'
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'spec file containing extra columns is not processed successfully')
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec file row 2 has too many columns',
                             'spec file containing extra columns produces an error log message')

    def test_spec_invalid_name_column(self):
        filename = 'spec_invalid_name_column.csv'
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'spec file containing invalid name is not processed successfully')
            # TODO - verify these two errors without depending on sequence in log output
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec column name .* is invalid.*',
                             'spec file containing invalid name column produces two error log messages')
            self.assertRegex(logged.output[1],
                             'ERROR:MunchSpecs:Spec file row.*has invalid name attribute',
                             'spec file containing invalid name column produces two error log messages')

    def test_spec_invalid_width_column(self):
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            filename = 'spec_invalid_width_column.csv'
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'spec file containing invalid width is not processed successfully')
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec file row 2 has invalid width attribute .*',
                             'spec file containing invalid width column produces an error log message')
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            filename = 'spec_invalid_width_negative_column.csv'
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'spec file containing invalid width is not processed successfully')
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec file row 2 has negative width attribute .*',
                             'spec file containing invalid width column produces an error log message')
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            filename = 'spec_invalid_width_empty_column.csv'
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'spec file containing invalid width is not processed successfully')
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec file row 2 has invalid width attribute .*',
                             'spec file containing invalid width column produces an error log message')

    def test_spec_invalid_datatype_column(self):
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            filename = 'spec_invalid_datatype_column.csv'
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'spec file containing invalid datatype is not processed successfully')
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec file row 2 has invalid datatype attribute .*',
                             'spec file containing invalid datatype column produces an error log message')
        with self.assertLogs('MunchSpecs', level='ERROR') as logged:
            filename = 'spec_invalid_datatype_empty_column.csv'
            self.assertFalse(self.munch_spec.process_spec_from_file(filename),
                             'spec file containing invalid datatype is not processed successfully')
            self.assertRegex(logged.output[0],
                             'ERROR:MunchSpecs:Spec file row 2 has invalid datatype attribute .*',
                             'spec file containing invalid datatype column produces an error log message')

    def test_spec_valid(self):
        """when a spec file is validated, the spec is persisted into the database"""
        filename = 'DATAspecvalid.csv'
        self.assertTrue(self.munch_spec.process_spec_from_file(filename),
                        'valid spec file is successfully processed')

        db = self.munch_spec.db
        import_format_row = db['import_format'].find_one(name='DATAspecvalid')

        self.assertTrue(import_format_row is not None,
                        'processing valid spec file results in a new row in import_format table')

        import_format_columns = db['import_format_column'].find(import_format_id=import_format_row['id'])

        self.assertEquals(import_format_columns is not None and
                          len(list(import_format_columns)), 2,
                          'processing valid spec file results in 2 rows added to the import_format_column table')

        self.assertEqual(self.munch_spec.processed_count, 1,
                         '1 spec file is processed')

        filename = 'DATApaddedbool.csv'
        self.assertTrue(self.munch_spec.process_spec_from_file(filename),
                        'valid spec file is successfully processed')

    def test_spec_load_from_db(self):
        """a Spec object is instantiated from a valid set of data for a given spec name
           loaded from database tables import_format and import_format_column"""
        self.munch_spec.process_spec_from_file('DATAspecvalid.csv')
        spec = self.munch_spec.load_spec_from_db('DATAspecvalid')

        self.assertIsNotNone(spec, 'loading spec from database produces a non-null spec object')
        self.assertEquals(len(spec.columns), 2,
                          'loading spec from database produces a spec with 2 columns')
        name_column = None
        valid_column = None

        for spec_column in spec.columns:
            if spec_column.name == 'name':
                name_column = spec_column
            elif spec_column.name == 'valid':
                valid_column = spec_column

        self.assertIsNotNone(name_column, 'loading spec from database returns spec with expected columns (name)')
        self.assertIsNotNone(valid_column, 'loading spec from database returns spec with expected columns (valid)')

    #
    # def test_spec_error_saving_to_db(self):
    #     """when an error occurs while saving the spec to the database, cleanup still takes place"""
    #     self.assertTrue(False)
    #
    # def test_spec_success_saving_to_db(self):
    #     """when a spec is validated and saved to the database, (...cleanup takes place...)"""
    #     self.assertTrue(False)
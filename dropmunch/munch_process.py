"""Usage: munch_process.py [-hvc -V]

Entry point for dropmunch
- ensures that this is the only dropmunch process in progress in the current working directory
- processes any new spec files
- processes any new data files that have a corresponding spec
- cleans up after itself

Options:
  -h --help
  -v      verbose (log level INFO)
  -V      more verbose (log level DEBUG). Warning - outputs DB credentials!
  -c      import_log.num_rows_processed will be updated after EVERY row!
          This allows dropmunch to recover from unexpected crashes to finish
          processing files, however it will slow down processing
"""

import os
import logging
from dropmunch import munch_data, munch_spec
from docopt import docopt


if __name__ == '__main__':
    arguments = docopt(__doc__)


class MunchProcess:
    def __init__(self, log_each_row=False):
        self.working_directory = os.getcwd()
        self.munch_spec = munch_spec.MunchSpec()
        self.munch_data = munch_data.MunchData(log_each_row=log_each_row)

    def get_pid_filename(self):
        return self.working_directory + '/.munching'

    def process_spec_files(self):
        self.munch_spec.process_spec_files()

    def process_data_files(self):
        self.munch_data.process_data_files()
        self.munch_data.cleanup()

def main():
    arguments = docopt(__doc__)

    if arguments['-v']:
      logging.basicConfig(level=logging.INFO)
    elif arguments['-V']:
      logging.basicConfig(level=logging.DEBUG)

    log_each_row = False

    if arguments['-c']:
        logging.info('import_log.num_rows_processed will be updated after EVERY row.')
        log_each_row = True

    munch_process = MunchProcess(log_each_row)
    pid_file = munch_process.get_pid_filename()

    if os.path.exists(pid_file):
        raise FileExistsError('Found .munching (pid file). is a munch process already running? Aborting ...')

    try:
        with open(pid_file, 'w'):
            munch_process.process_spec_files()
            munch_process.process_data_files()
    except Exception as e:
        logging.error('An error occurred while running the munch process : {0}'.format(e))
    finally:
        os.remove(pid_file)

main()

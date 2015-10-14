import unittest


class ProcessBehavior(unittest.TestCase):
    """Test behavior of the munch process"""

    def test_process_already_running(self):
        """when the PID file indicates that a munch process is currently running, an error is raised"""
        self.assertTrue(False)

    def test_process_creates_pid_file(self):
        """when the munch process starts, it creates a PID file containing the process id"""
        self.assertTrue(False)

    def test_process_performs_subtasks(self):
        """the munch process performs all of the specified subtasks"""
        self.assertTrue(False)

    def test_process_nonfatal_subtask_error(self):
        """when a specific munch subtask fails with a non-fatal error, remaining subtasks are performed"""
        self.assertTrue(False)

    def test_process_fatal_subtask_error(self):
        """when a munch subtask has a fatal error, remaining subtasks aren't performed, but cleanup is performed"""
        self.assertTrue(False)

    def test_process_cleans_up_pid_file(self):
        """when the munch process is stopping, it first cleans up the PID file"""
        self.assertTrue(False)

    def test_process_pid_cleanup_fails(self):
        """when the munch process fails to clean up its PID file, it raises an error"""
        self.assertTrue(False)
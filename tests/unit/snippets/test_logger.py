import unittest
from pyiron_workflow.snippets.logger import logger
import os
import shutil


class TestLogger(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.logger_file = os.path.join(os.getcwd(), 'pyiron.log')
        cls.backup_file = os.path.join(os.getcwd(), 'pyiron.log.test_logger_backup')
        shutil.copy(cls.logger_file, cls.backup_file)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.move(cls.backup_file, cls.logger_file)

    def test_logger(self):
        logsize = os.path.getsize(self.logger_file)
        logger.warning("Here is a warning")
        self.assertGreater(os.path.getsize(self.logger_file), logsize)

    def test_set_logging_level(self):
        logger.set_logging_level(10)
        self.assertEqual(10, logger.getEffectiveLevel(), "Overall logger level should match input")
        self.assertEqual(10, logger.handlers[0].level, "Stream level should match input")
        self.assertEqual(10, logger.handlers[0].level, "File level should match input")

        logger.set_logging_level(20, channel=1)
        self.assertEqual(10, logger.getEffectiveLevel(), "Overall logger level should not have changed")
        self.assertEqual(10, logger.handlers[0].level, "Stream level should not have changed")
        self.assertEqual(20, logger.handlers[1].level, "File level should match input")

        logger.set_logging_level("WARNING", channel=0)
        self.assertEqual(30, logger.handlers[0].level, "Should be able to set by string")


if __name__ == '__main__':
    unittest.main()

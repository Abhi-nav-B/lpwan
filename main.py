import logging
import os.path
import shutil
from constants import RESULT_PATH
import tests_to_execute
import tests_execution

logger = logging.getLogger('my_logger')

if __name__ == '__main__':
    for test in tests_to_execute.testcase_list:
        if os.path.isdir(rf'{RESULT_PATH}\{test['tp_number']}'):
            shutil.rmtree(rf'{RESULT_PATH}\{test['tp_number']}')
        os.makedirs(rf'{RESULT_PATH}\{test['tp_number']}')

        # clear all logger handles if any
        logger.handlers.clear()

        # start test execution
        tests_execution.execute_test(**test)

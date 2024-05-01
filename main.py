import os.path
import shutil
import logging
import tests_to_execute
import tests_execution
from constants import RESULT_PATH

logger = logging.getLogger('my_logger')


if __name__ == '__main__':
    # api services and meter functionality
    for test in tests_to_execute.testcase_list:
        if os.path.isdir(rf'{RESULT_PATH}\{test['tp_number']}'):
            try:
                shutil.rmtree(rf'{RESULT_PATH}\{test['tp_number']}')
            except PermissionError:
                print('Close all the open files from the result folder.')
            except Exception as e:
                print(e)
        os.makedirs(rf'{RESULT_PATH}\{test['tp_number']}')

        # clear all logger handlers if there is any
        logger.handlers.clear()

        # start test execution
        tests_execution.execute_test(**test)

    # import time
    # from common_fn import check_for_execution
    #
    # # smarts and api service integration
    # while True:
    #     args = check_for_execution()
    #     header = list(args[-1].keys())
    #     values = list(args[-1].values())
    #     args.pop()
    #     args.append(header)
    #     args.append(values)
    #     tests_execution.call_service(*args)
    #     time.sleep(60)

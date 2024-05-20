import sys
import os
import time
import logging
import datetime
import json
import excel_manangement as em


logger = logging.getLogger('my_logger')


def start_logger(logger_path: str):
    if logger_path[logger_path.find('.', 1):].lower() in ['.txt', '.log']:
        filename = logger_path
    else:
        default_file_name = f'{datetime.datetime.now().strftime("%Y%m%d")}.log'
        filename = os.path.join(logger_path, default_file_name)

    lgr = logging.getLogger('my_logger')
    lgr.setLevel(logging.INFO)

    # create file handler that logs debug and higher level messages
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.INFO)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        fmt='{asctime} | {filename:18} | Line {lineno:4} | {levelname:8} | {message}',
        style='{')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # add the handlers to logger
    lgr.addHandler(ch)
    lgr.addHandler(fh)

    time.sleep(2)


def exception_log(e):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logger.error((exc_type, f_name, f'Line: {exc_tb.tb_lineno}'))
    logger.error(e)


def write_in_file(issue, in_detail):
    try:
        with open('\\error_log.txt', 'w') as f:
            f.writelines(issue)
            f.writelines(in_detail)

    except Exception as e:
        exception_log(e)


def get_hes_id():
    try:
        if not os.path.exists(r'.\supporting'):
            os.makedirs(r'.\supporting')

            with open(r'supporting/hes_id', 'w') as f:
                f.write('1')
                return 1

        else:
            with open(r'supporting/hes_id', 'r') as f:
                hes_id = int(f.read())
                hes_id += 1

            with open(r'supporting/hes_id', 'w') as f:
                f.write(str(hes_id))

            return hes_id

    except Exception as e:
        exception_log(e)


def countdown(t):
    t = int(t)
    while t:  # while t > 0 for clarity
        minutes = t // 60
        seconds = t % 60
        timer = '{:02d}:{:02d}'.format(minutes, seconds)
        print(timer, end="\r")  # overwrite previous line
        time.sleep(1)
        t -= 1


def duration(func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        func(*args, **kwargs)
        logger.info(f'Time took to execute: {time.strftime("%H:%M:%S", time.gmtime(time.time() - t1))}')

    return wrapper


def check_for_execution() -> list | bool:
    execute_script = False
    payload = False
    with open(r'.\supporting\sample.txt', 'r') as f:
        for i, line in enumerate(f):
            if i == 0 and '1' in line:
                execute_script = True
            if i == 1 and 'None\n' not in line:
                payload_data = line
                payload = True
        if execute_script and payload:
            temp = payload_data.split()
            temp[-1] = json.loads(temp[-1])
            return temp
        else:
            return_to_smarts(False)
            return False


def return_to_smarts(status: bool):
    with open(r'.\supporting\sample.txt', 'w') as f:
        f.write(f'0\nNone\n{status}')


def xl_data_to_list(data_from_xl) -> list:
    new_list = list()

    for item in data_from_xl:
        item = str(item)
        item = item[item.find(':') + 1:].replace("'", '')
        new_list.append(item)

    return new_list


def time_in_seconds(date_time: str):
    if len(date_time) == 10:
        obj_datetime = datetime.datetime.strptime(date_time, '%d/%m/%Y')
    elif len(date_time) == 19:
        obj_datetime = datetime.datetime.strptime(date_time, '%d/%m/%Y %H:%M:%S')
    else:
        raise Exception(f'Invalid date time string.')

    return int((obj_datetime - datetime.datetime(1970, 1, 1)).total_seconds()) * 1000


def dict_filter(response_dict: dict, kw_filter: list, is_flat: bool = True):
    try:
        # Clearing global list_of_dict before calling get_all_keys_values
        em.list_of_dict.clear()
        filtered_list = []
        if is_flat:
            response_dict = em.get_all_keys_values(response_dict)
            if kw_filter is not None:
                # Iterate over each dictionary in the list
                for element in kw_filter:
                    # Check each key in the current dictionary
                    for item in response_dict:
                        # If the key is in kw_filter, add it to the filtered_dict
                        if element in item.keys():
                            filtered_list.append(item)
                            response_dict.remove(item)
                            break
        else:
            if kw_filter is not None:
                # Iterate over each dictionary in the list
                for element in kw_filter:
                    # Check each key in the current dictionary
                    for item in response_dict:
                        # If the key is in kw_filter, add it to the filtered_dict
                        if element == item:
                            filtered_list.append({item: response_dict[item]})
                            # response_dict.remove(item)
                            break
        return filtered_list
    except Exception as e:
        print(e)


def add_prefix_in_key(d, prefix):
    return {f"{prefix}_{k}": v for k, v in d.items()}


data_kr_list = []


def key_rename(data, parent_key=None) -> list[{str, str}]:
    for k, v in data.items():
        current_key = f"{parent_key}_{k}" if parent_key else k
        if type(v) is dict:
            key_rename(v, current_key)
        else:
            data_kr_list.append({current_key: v})
    return data_kr_list


def convert_timestamp(input_str):              # -->DD/MM/YYYY HH:MM:SS
    # Find the index of the first '(' and the last ')'
    start_index = input_str.find('(') + 1
    end_index = input_str.rfind(')')

    # Extract the timestamp substring
    timestamp_str = input_str[start_index:end_index]

    # Convert timestamp string to integer
    timestamp = int(timestamp_str)

    # Convert timestamp to datetime
    formatted_time = datetime.datetime.fromtimestamp(timestamp / 1000).strftime('%d/%m/%Y %H:%M:%S')

    return formatted_time


if __name__ == '__main__':
    print(get_hes_id())

import json
import requests
import logging
import service_payload as sp
import excel_manangement as em
from requests.auth import HTTPBasicAuth
from constants import *
from common_fn import exception_log, countdown, duration, xl_data_to_list

logger = logging.getLogger('my_logger')


def post_request(service_name: str, json_data: dict):
    url = END_POINT_URL + f'/{service_name}'
    # print(f'Post Request: {url}')
    logger.info(f'Post Request: {url}')

    try:
        response = requests.post(url=url, json=json_data, auth=HTTPBasicAuth(USER, PASSWORD))
        return response

    except Exception as e:
        exception_log(e)
        return False


def get_request(service_name: str, wse_id: str):
    url = END_POINT_URL + f'/{service_name}?WseId={wse_id}'
    # print(f'Get Request: {url}')
    logger.info(f'Get Request: {url}')
    try:
        while True:
            response = requests.get(url=url, auth=HTTPBasicAuth(USER, PASSWORD))

            if response.headers['X-StatusCode'] == '0':
                # print(WEB_SERVICE_STATUS_CODE[int(response.headers['X-StatusCode'])])
                logger.info(WEB_SERVICE_STATUS_CODE[int(response.headers['X-StatusCode'])])
                return response

            if any(lower <= int(response.headers['X-StatusCode']) <= upper for (lower, upper) in
                   [(4, 8), (60, 63), (80, 88), (100, 112), (116, 319), (400, 409), (600, 612), (700, 730),
                    (1000, 1009)]):
                # print(WEB_SERVICE_STATUS_CODE[int(response.headers['X-StatusCode'])])
                logger.info(WEB_SERVICE_STATUS_CODE[int(response.headers['X-StatusCode'])])
                return WEB_SERVICE_STATUS_CODE[int(response.headers['X-StatusCode'])]  # response.status_code

            countdown(5)

    except Exception as e:
        exception_log(e)
        return False


def get_task_reply(wse_id: str):
    try:
        response = get_request('GetTaskReply', wse_id)
        # print(response)
        logger.info(response)

    except Exception as e:
        exception_log(e)


@duration
def set_schedule(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__}')
        logger.info(f'Service called: {set_schedule.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = data_from_xl['ScheduledTasks']
        payload = sp.set_schedule_payload(meter, spn, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('SetSchedule', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_schedule(tp_number, iteration, spn, meter)
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_schedule(tp_number: str, iteration: int, spn: str, meter: str):
    try:
        # print(f'Service called: {get_schedule.__name__}')
        logger.info(f'Service called: {get_schedule.__name__}')

        response = post_request('GetSchedule', sp.get_schedule_payload(spn, meter))
        get_schedule_reply(tp_number, iteration, int(response.text))

    except Exception as e:
        exception_log(e)


def get_schedule_reply(tp_number, iteration, wse_id):
    try:
        # print(f'Service called: {get_schedule_reply.__name__}')
        logger.info(f'Service called: {get_schedule_reply.__name__}')

        response = get_request('GetScheduleReply', wse_id)
        response_dict = json.loads(response.text)

        # print(f'Received Data: {response.text}')
        logger.info(f'Received Data: {response.text}')

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, response_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.')
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def get_supply_control(tp_number, iteration, header: list, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {get_Supply_Control.__name__}')
        logger.info(f'Service called: {get_supply_control.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        payload = sp.get_Supply_Control_payload(meter, spn, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')
        dict_to_update.pop('AutoTransfer')

        # write reference data into excel
        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('GetSupplyControlCode', payload)
        if response.status_code in [200, 202]:
            get_supply_control_code_reply(tp_number, iteration, response.text)
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_supply_control_code_reply(tp_number, iteration, wse_id):
    try:
        # print(f'Service called: {get_supply_control_code_reply.__name__}')
        logger.info(f'Service called: {get_supply_control_code_reply.__name__}')

        wse_id = json.loads(wse_id)['WseId']

        response = get_request('GetSupplyControlCodeReply', wse_id)
        response_dict = json.loads(response.text)

        # print(f'Received Data: {response.text}')
        logger.info(f'Received Data: {response.text}')

        # remove not required dict items
        response_dict.pop('ProcessTime')

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, response_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.')
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def change_billing_dates(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_billing_dates.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = json.loads(data_from_xl['BillingPeriod'].replace('\\n', ''))

        payload = sp.change_billing_dates_payload(meter, spn, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeBillingDates', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            # get_meter_configuration(tp_number, iteration, meter, spn)
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))
    except Exception as e:
        exception_log(e)

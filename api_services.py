import json
import datetime
import requests
import logging
import copy
import service_payload as sp
import excel_management as em
import sys  # TO STOP SCRIPT EXECUTION
from requests.auth import HTTPBasicAuth
from constants import *
from common_fn import (exception_log, countdown, duration, xl_data_to_list, dict_filter, add_prefix_in_key, key_rename,
                       convert_timestamp, time_in_seconds, get_hes_id)

# logger = logging.getLogger(__name__)
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
        return response
    except Exception as e:
        exception_log(e)


@duration
def set_schedule(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {set_schedule.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = data_from_xl['ScheduledTasks']
        payload = sp.set_schedule_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)
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


def get_schedule(tp_number, iteration, spn, meter):
    try:
        # print(f'Service called: {get_schedule.__name__})
        logger.info(f'Service called: {get_schedule.__name__}')

        response = post_request('GetSchedule', sp.get_schedule_payload(spn, meter))
        get_schedule_reply(tp_number, iteration, int(response.text))

    except Exception as e:
        exception_log(e)


def get_schedule_reply(tp_number, iteration, wse_id):
    try:
        # print(f'Service called: {get_schedule_reply.__name__})
        logger.info(f'Service called: {get_schedule_reply.__name__}')

        response = get_request('GetScheduleReply', wse_id)
        response_dict = json.loads(response.text)

        # print(f'Received Data: {response.text}')
        logger.info(f'Received Data: {response.text}')

        em.list_of_dict.clear()  # clear old data if there is any
        response_dict = em.get_all_keys_values(response_dict)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, response_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.)
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def get_supply_control(tp_number, iteration, header: list, data_from_xl: list, spn: str, meter: str):
    try:
        logger.info(f'Service called: {get_supply_control.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        payload = sp.get_supply_control_payload(spn, meter, data_from_xl)

        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')
        dict_to_update.pop('AutoTransfer')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

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
        logger.info(f'Service called: {get_supply_control_code_reply.__name__}')

        wse_id = json.loads(wse_id)['WseId']

        response = get_request('GetSupplyControlCodeReply', wse_id)
        response_dict = json.loads(response.text)

        # print(f'Received Data: {response.text}')
        logger.info(f'Received Data: {response.text}')

        # remove not required dict items
        response_dict.pop('ProcessTime')

        em.list_of_dict.clear()  # clear old data if there is any
        response_dict = em.get_all_keys_values(response_dict)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, response_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def change_price(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_price.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = json.loads(data_from_xl['PriceConfig'])
        # data_from_xl['PriceConfig'] = json.loads(data_from_xl['PriceConfig'])
        payload = sp.change_price_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangePrice', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)

            get_meter_configuration(tp_number, iteration, spn, meter, 'ChangePrice',
                                    [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


@duration
def change_prepayment_configuration(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_prepayment_configuration.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = json.loads(data_from_xl['PrepaymentConfig'])
        # data_from_xl['PrepaymentConfig'] = json.loads(data_from_xl['PrepaymentConfig'])
        payload = sp.change_prepayment_configuration_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangePrepaymentConfiguration', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_configuration(tp_number, iteration, spn, meter, 'PrepaymentConfig',
                                    [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


@duration
def change_gas_parameters(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_gas_parameters.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = json.loads(data_from_xl['GasConfig'])
        # data_from_xl['PrepaymentConfig'] = json.loads(data_from_xl['PrepaymentConfig'])
        payload = sp.change_gas_parameters_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeGasParameters', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_configuration(tp_number, iteration, spn, meter, 'GasConfig',
                                    [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

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

        payload = sp.change_billing_dates_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeBillingDates', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_configuration(tp_number, iteration, spn, meter, 'BillingPeriod',
                                    [list(item.keys())[0] for item in dict_to_update])

        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


@duration
def change_tariff_plan(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_tariff_plan.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['TariffScheme', 'PrepaymentConfig', 'BillingPeriod']
        data_from_xl = {x: data_from_xl[x] for x in keys}

        data_from_xl['TariffScheme'] = json.loads(data_from_xl['TariffScheme'])
        # if 'PrepaymentConfig' in data_from_xl:
        data_from_xl['PrepaymentConfig'] = json.loads(data_from_xl['PrepaymentConfig'])
        data_from_xl['BillingPeriod'] = json.loads(data_from_xl['BillingPeriod'])

        payload = sp.change_tariff_plan_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeTariffPlan', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_configuration(tp_number, iteration, spn, meter, 'TariffPlan',
                                    [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


@duration
def initialise_meter(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {initialise_meter.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['DisconnectionAllowed', 'TariffScheme', 'PrepaymentConfig', 'BillingPeriod', 'GasConfig',
                'ImportCO2Config', 'ExportCO2Config']
        data_from_xl = {x: data_from_xl[x] for x in keys if data_from_xl.get(x, '') != ''}

        data_from_xl['TariffScheme'] = json.loads(data_from_xl['TariffScheme'])
        if 'PrepaymentConfig' in data_from_xl:
            data_from_xl['PrepaymentConfig'] = json.loads(data_from_xl['PrepaymentConfig'])
        data_from_xl['BillingPeriod'] = json.loads(data_from_xl['BillingPeriod'])
        if 'GasConfig' in data_from_xl:
            data_from_xl['GasConfig'] = json.loads(data_from_xl['GasConfig'])
        if 'ImportCO2Config' in data_from_xl:
            data_from_xl['ImportCO2Config'] = json.loads(data_from_xl['ImportCO2Config'])
        if 'ExportCO2Config' in data_from_xl:
            data_from_xl['ExportCO2Config'] = json.loads(data_from_xl['ExportCO2Config'])

        payload = sp.initialise_meter_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('InitialiseMeter', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_configuration(tp_number, iteration, spn, meter, 'InitialiseMeter',
                                    [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


@duration
def change_co2_configuration(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_co2_configuration.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['ImportCO2Config', 'ExportCO2Config']
        data_from_xl = {x: data_from_xl[x] for x in keys if data_from_xl.get(x, '') != ''}

        if 'ImportCO2Config' in data_from_xl:
            data_from_xl['ImportCO2Config'] = json.loads(data_from_xl['ImportCO2Config'])
        if 'ExportCO2Config' in data_from_xl:
            data_from_xl['ExportCO2Config'] = json.loads(data_from_xl['ExportCO2Config'])

        payload = sp.change_co2_configuration_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeCO2Configuration', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_configuration(tp_number, iteration, spn, meter, 'CO2Config',
                                    [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


@duration
def change_disconnection_setting(tp_number, iteration, spn: str, meter: str):
    try:
        logger.info(f'Service called: {change_disconnection_setting.__name__}')

        payload = sp.change_disconnection_setting_payload(spn, meter, True)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeDisconnectionSetting', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_configuration(tp_number, iteration, spn, meter, 'DisconnectionAllowed',
                                    [list(item.keys())[0] for item in dict_to_update])

        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_meter_configuration(tp_number, iteration, spn: str, meter: str, dict_name: str | None = None,
                            kw_filter: list = None):
    try:
        # print(f'Service called: {get_Supply_Control.__name__})
        logger.info(f'Service called: {get_meter_configuration.__name__}')

        # get payload for service
        payload = sp.get_meter_configuration_payload(spn, meter, False)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        response = post_request('GetMeterConfiguration', payload)
        if response.status_code in [200, 202]:
            get_meter_configuration_reply(tp_number, iteration, response.text, dict_name, kw_filter)
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_meter_configuration_reply(tp_number, iteration, wse_id, dict_name: str | None = None, kw_filter: list = None):
    try:
        # print(f'Service called: {get_supply_control_code_reply.__name__})
        logger.info(f'Service called: {get_meter_configuration_reply.__name__}')

        response = get_request('GetMeterConfigurationReply', wse_id)
        response_dict = json.loads(response.text)

        logger.info(f'Received Data: {response.text}')

        new_dict = dict()
        key = None
        match dict_name:
            case 'PrepaymentConfig':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'PrepaymentConfig']

            case 'BillingPeriod':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'BillingPeriod']

            case 'TariffPlan':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'TariffScheme', 'PrepaymentConfig', 'BillingPeriod']

            case 'ChangePrice':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'TariffScheme']
                response_dict["TariffScheme"]["Import"]["Tou"]["ImportPrices"] \
                    = response_dict["TariffScheme"]["Import"]["Tou"]["TierPrices"]
                del response_dict["TariffScheme"]["Import"]["Tou"]["TierPrices"]
                response_dict["TariffScheme"]["Export"]["Tou"]["ExportPrices"] \
                    = response_dict["TariffScheme"]["Export"]["Tou"]["TierPrices"]
                del response_dict["TariffScheme"]["Export"]["Tou"]["TierPrices"]

            case 'InitialiseMeter':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'DisconnectionAllowed', 'TariffScheme',
                       'PrepaymentConfig', 'BillingPeriod', 'GasConfig', 'ImportCO2Config', 'ExportCO2Config']

            case 'GasConfig':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'GasConfig']

            case 'CO2Config':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'ImportCO2Config', 'ExportCO2Config']

            case 'DisconnectionAllowed':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'DisconnectionAllowed']

        new_dict.update({element: response_dict[element] for element in key if response_dict.get(element, '') != ''})

        filtered_dict = dict_filter(new_dict, kw_filter)

        for i, item in enumerate(filtered_dict):
            if list(item.keys())[0] == 'LoadLimitBelowCutOff':
                item.update({list(item.keys())[0]: float(list(item.values())[0])})
            elif list(item.keys())[0] == 'StandingCharge':
                item.update({list(item.keys())[0]: float(list(item.values())[0])})
            elif list(item.keys())[0] == 'DomesticUsagePercentage':
                item.update({list(item.keys())[0]: float(list(item.values())[0])})
            elif list(item.keys())[0] == 'DomesticFuelTaxRate':
                item.update({list(item.keys())[0]: float(list(item.values())[0])})
            elif list(item.keys())[0] == 'CommercialFuelTaxRate':
                item.update({list(item.keys())[0]: float(list(item.values())[0])})
            elif list(item.keys())[0] == 'ImportPrices':
                filtered_dict[i]['ImportPrices'] = [float(element) for element in filtered_dict[i]['ImportPrices']]
            elif list(item.keys())[0] == 'ExportPrices':
                filtered_dict[i]['ExportPrices'] = [float(element) for element in filtered_dict[i]['ExportPrices']]
            elif list(item.keys())[0] == 'TierPrices':
                filtered_dict[i]['TierPrices'] = [float(element) for element in filtered_dict[i]['TierPrices']]
            elif list(item.keys())[0] == 'CO2Factor':
                item.update({list(item.keys())[0]: float(list(item.values())[0])})

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.)
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def change_event_configuration(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_event_configuration.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = data_from_xl['EventConfig']
        payload = sp.change_event_configuration_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')
        dict_to_update['EventConfig'] = [add_prefix_in_key(item, item.get('EventCode')) for item in
                                         dict_to_update['EventConfig']]

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeEventConfiguration', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_event_configuration(tp_number, iteration, spn, meter, [list(item.keys())[0] for item in dict_to_update])
        else:
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_event_configuration(tp_number, iteration, spn, meter, kw_filter: list = None):
    try:
        # print(f'Service called: {get_schedule.__name__})
        logger.info(f'Service called: {get_event_configuration.__name__}')

        response = post_request('GetEventConfiguration', sp.get_event_configuration_payload(spn, meter))
        get_event_configuration_reply(tp_number, iteration, int(response.text), kw_filter)

    except Exception as e:
        exception_log(e)


def get_event_configuration_reply(tp_number, iteration, wse_id, kw_filter: list = None):
    try:
        # print(f'Service called: {get_schedule_reply.__name__})
        logger.info(f'Service called: {get_event_configuration_reply.__name__}')

        response = get_request('GetEventConfigurationReply', wse_id)
        response_dict = json.loads(response.text)

        # print(f'Received Data: {response.text}')
        logger.info(f'Received Data: {response.text}')
        response_dict['EventConfig'] = \
            [add_prefix_in_key(item, item.get('EventCode')) for item in response_dict['EventConfig']]

        filtered_dict = dict_filter(response_dict, kw_filter)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.)
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def change_profile_configuration(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_profile_configuration.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))
        # data_from_xl = {k: v for k, v in data_from_xl.items() if k != '' and v != ''}

        # get payload for service
        keys = ['StandardProfile', 'DiagnosticProfile']
        data_from_xl = {x: data_from_xl[x] for x in keys if data_from_xl.get(x, '') != ''}

        data_from_xl['StandardProfile'] = json.loads(data_from_xl['StandardProfile'])
        if 'DiagnosticProfile' in data_from_xl:
            data_from_xl['DiagnosticProfile'] = json.loads(data_from_xl['DiagnosticProfile'])
        payload = sp.change_profile_configuration_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeProfileConfiguration', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_profile_configuration(tp_number, iteration, spn, meter,
                                      [list(item.keys())[0] for item in dict_to_update])
        else:
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_profile_configuration(tp_number, iteration, spn, meter, kw_filter: list = None):
    try:
        # print(f'Service called: {get_schedule.__name__})
        logger.info(f'Service called: {get_profile_configuration.__name__}')

        response = post_request('GetProfileConfiguration', sp.get_profile_configuration_payload(spn, meter))
        get_profile_configuration_reply(tp_number, iteration, int(response.text), kw_filter)

    except Exception as e:
        exception_log(e)


def get_profile_configuration_reply(tp_number, iteration, wse_id, kw_filter: list = None):
    try:
        # print(f'Service called: {get_schedule_reply.__name__})
        logger.info(f'Service called: {get_profile_configuration_reply.__name__}')

        response = get_request('GetProfileConfigurationReply', wse_id)
        response_dict = json.loads(response.text)
        response_dict['StandardProfile']['ParameterCodes'].sort()
        response_dict['DiagnosticProfile']['ParameterCodes'].sort()

        # print(f'Received Data: {response.text}')
        logger.info(f'Received Data: {response.text}')
        filtered_dict = dict_filter(response_dict, kw_filter)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.)
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def send_text_message(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {send_text_message.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['MessageType', 'DisplayTime', 'Duration', 'Contents']
        data_from_xl = {x: data_from_xl[x] for x in keys}

        payload = sp.send_text_message_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        # em.list_of_dict.clear()  # clear old data if there is any
        # dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('SendTextMessage', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_text_message_receipt(tp_number, iteration, spn, meter, 'TariffPlan',
                                     [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


@duration
def change_system_parameters(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_system_parameters.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl['ParamSettings'] = json.loads(data_from_xl['ParamSettings'])
        payload, payload_list = sp.change_system_parameters_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')
        dict_to_update['ParamSettings'] = key_rename(dict_to_update['ParamSettings'])

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeSystemParameters', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_system_parameters(tp_number, iteration, spn, meter, [list(item.keys())[0] for item in dict_to_update],
                                  payload_list)
        else:
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_system_parameters(tp_number, iteration, spn, meter, kw_filter: list = None, payload_list=None):
    try:
        # print(f'Service called: {get_schedule.__name__})
        logger.info(f'Service called: {get_system_parameters.__name__}')

        response = post_request('GetSystemParameters', sp.get_system_parameters_payload(spn, meter))
        get_system_parameters_reply(tp_number, iteration, int(response.text), kw_filter, payload_list)

    except Exception as e:
        exception_log(e)


def get_system_parameters_reply(tp_number, iteration, wse_id, kw_filter: list = None, payload_list=None):
    try:
        # print(f'Service called: {get_schedule_reply.__name__})
        logger.info(f'Service called: {get_system_parameters_reply.__name__}')

        response = get_request('GetSystemParametersReply', wse_id)
        response_dict = json.loads(response.text)
        response_list = list(response_dict['ParamSettings'].keys())
        response_list.sort()
        # response_result = {i: response_dict['ParamSettings'][i] for i in response_list}
        # response_dict['ParamSettings'] = dict_filter(response_result, payload_list, False)
        response_dict['ParamSettings'] = key_rename(response_dict['ParamSettings'])

        logger.info(f'Received Data: {response.text}')
        filtered_dict = dict_filter(response_dict, kw_filter)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.)
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def change_demand_limit_configuration(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        logger.info(f'Service called: {change_demand_limit_configuration.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = data_from_xl['DemandLimitConfig']

        payload = sp.change_demand_limit_configuration_payload(spn, meter, data_from_xl)

        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeDemandLimitConfiguration', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_demand_limit_configuration(tp_number, iteration, spn, meter,
                                           [list(item.keys())[0] for item in dict_to_update])
        else:
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_demand_limit_configuration(tp_number, iteration, spn, meter, kw_filter: list = None):
    try:
        logger.info(f'Service called: {get_demand_limit_configuration.__name__}')

        response = post_request('GetDemandLimitConfiguration',
                                sp.get_demand_limit_configuration_payload(spn, meter))
        get_demand_limit_configuration_reply(tp_number, iteration, int(response.text), kw_filter)

    except Exception as e:
        exception_log(e)


def get_demand_limit_configuration_reply(tp_number, iteration, wse_id, kw_filter: list = None):
    try:
        # print(f'Service called: {get_schedule_reply.__name__})
        logger.info(f'Service called: {get_demand_limit_configuration_reply.__name__}')

        response = get_request('GetDemandLimitConfigurationReply', wse_id)
        response_dict = json.loads(response.text)

        logger.info(f'Received Data: {response.text}')

        filtered_dict = dict_filter(response_dict, kw_filter)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.)
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def change_data_disclosure_settings(tp_number, iteration, spn: str, meter: str):
    try:
        logger.info(f'Service called: {change_data_disclosure_settings.__name__}')

        payload = sp.change_data_disclosure_settings_payload(spn, meter, False)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeDataDisclosureSettings', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_diagnostic_data(tp_number, iteration, spn, meter, 'RestrictDataOnDisplay', [list(item.keys())[0]
                                                                                                  for item in
                                                                                                  dict_to_update])

        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def set_payment_card_id(tp_number, iteration, spn: str, meter: str):
    try:
        data_in_json = get_site_information(tp_number, iteration, spn)
        for item in data_in_json['DeviceList']:
            if 'PaymentCardId' in item:
                payment_card_id = item['PaymentCardId']
                
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {set_payment_card_id.__name__}')

        payload = sp.set_payment_card_id_payload(spn, meter, payment_card_id)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 7)

        response = post_request('SetPaymentCardId', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_diagnostic_data(tp_number, iteration, spn, meter, 'PaymentCardId',
                                      [list(item.keys())[0] for item in dict_to_update], 9)

        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_site_information(tp_number, iteration, spn):
    try:
        logger.info(f'Service called: {get_site_information.__name__}')

        response = post_request('GetSiteInformation',
                                sp.get_site_information_payload(spn))
        response_dict = json.loads(response.text)

        logger.info(f'Received Data: {response.text}')

        return response_dict

    except Exception as e:
        exception_log(e)


def get_payment_card_information(tp_number, iteration, spn, kw_filter: list = None):
    try:
        data_in_json = get_site_information(tp_number, iteration, spn)
        for item in data_in_json['DeviceList']:
            if 'PaymentCardId' in item:
                payment_card_id = item['PaymentCardId']
        logger.info(f'Service called: {get_payment_card_information.__name__}')

        response = post_request('GetPaymentCardInformation',
                                sp.get_payment_card_information_payload(payment_card_id))
        response_dict = json.loads(response.text)
        logger.info(f'Received Data: {response.text}')

        response_dict.update({'PaymentCardId': payment_card_id})

        filtered_dict = dict_filter(response_dict, kw_filter)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.)
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


def deregister_payment_card(tp_number, iteration, spn):
    try:
        data_in_json = get_site_information(tp_number, iteration, spn)
        for item in data_in_json['DeviceList']:
            if 'PaymentCardId' in item:
                payment_card_id = item['PaymentCardId']

                logger.info(f'Service called: {deregister_payment_card.__name__}')
                response = post_request('DeregisterPaymentCard', sp.deregister_payment_card_payload(payment_card_id))

                logger.info(STATUS_CODE.get(response.status_code))
    except Exception as e:
        exception_log(e)


@duration
def set_and_register_payment_card_id(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:

        # Deregister PaymentCard id if PaymentCard id is present in Get Site Information
        deregister_payment_card(tp_number, iteration, spn)

        # Register new PaymentCard id which will be set in meter
        logger.info(f'Service called: {set_and_register_payment_card_id.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['PaymentCardId', 'CustomerName', 'VatGroupNo']
        data_from_xl = {x: data_from_xl[x] for x in keys}

        payload = sp.register_payment_card_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('RegisterPaymentCard', payload)
        if response.status_code in [200, 202]:
            get_payment_card_information(tp_number, iteration, spn, [list(item.keys())[0] for item in dict_to_update])
        else:
            logger.info(STATUS_CODE.get(response.status_code))

        # Get Meter diagnostic data for Meter mode
        data_in_json = get_meter_diagnostic_data(tp_number, iteration, spn, meter, 'PaymentCardId',
                                                 None)
        meter_mode = data_in_json['MeterData']['MeterMode']
        # if meter mode is Credit then change meter mode to Prepayment by Change Tariff Plan
        if meter_mode == 0:
            # print(f'Service called: {set_schedule.__name__})
            logger.info(f'Service called: {change_tariff_plan.__name__}')

            payload = {
                'HesId': get_hes_id(),
                'SupplyType': 0,
                'ServicePointNo': spn,
                'DeviceNo': meter,
                'Priority': 1,
                'TariffScheme': {'MeterMode': 1, 'TariffType': 2, 'TariffPlan': 'importExport', 'TariffLabel':
                                 'ImportExport', 'CurrencyCode': 36, 'StandingCharge': 500.0, 'TaxConfig':
                                 {'DomesticUsagePercentage': 100.0, 'DomesticFuelTaxRate': 5.0,
                                  'CommercialFuelTaxRate': 0.0}, 'Import': {'ChargingType': 1, 'Tou':
                                                                            {'TierCount': 4, 'TierLabels':
                                                                             ['Prepay', 'Prepay1', 'Prepay2',
                                                                              'Prepay3'],
                                                                             'TierPrices': [200.0, 400.0, 600.0, 800.0]}
                                                                            },
                                 'Export': {'ChargingType': 1, 'Tou': {'TierCount': 4, 'TierLabels':
                                                                       ['Export1', 'Export2', 'Export3', 'Export4'],
                                                                       'TierPrices': [100.0, 200.0, 300.0, 400.0]}},
                                 'TouCalendar': {'Name': 'NewClandar', 'TimeReference': 0, 'Seasons':
                                                 [{'StartDate': {'Month': 2, 'Day': 26}, 'WeekIndex': 0}], 'Weeks':
                                                 [{'Monday': 0, 'Tuesday': 0, 'Wednesday': 0, 'Thursday': 0,
                                                   'Friday': 0, 'Saturday': 1, 'Sunday': 1}],
                                                 'SpecialDays': [{'Date': {'Month': 3, 'Day': 15}, 'DayIndex': 0}],
                                                 'Days': [{'TouSwitches': [
                                                     {'StartTime': {'Hour': 0, 'Minute': 0, 'Second': 0},
                                                      'ImportTierIndex': 0, 'ExportTierIndex': 0},
                                                     {'StartTime': {'Hour': 6, 'Minute': 0, 'Second': 0},
                                                      'ImportTierIndex': 1, 'ExportTierIndex': 1},
                                                     {'StartTime': {'Hour': 12, 'Minute': 0, 'Second': 0},
                                                      'ImportTierIndex': 2, 'ExportTierIndex': 2},
                                                     {'StartTime': {'Hour': 18, 'Minute': 0, 'Second': 0},
                                                      'ImportTierIndex': 3, 'ExportTierIndex': 3}]},
                                                     {'TouSwitches':
                                                      [{'StartTime': {'Hour': 0, 'Minute': 0, 'Second': 0},
                                                        'ImportTierIndex': 0, 'ExportTierIndex': 0},
                                                       {'StartTime': {'Hour': 4, 'Minute': 0, 'Second': 0},
                                                        'ImportTierIndex': 2, 'ExportTierIndex': 2},
                                                       {'StartTime': {'Hour': 8, 'Minute': 0, 'Second': 0},
                                                        'ImportTierIndex': 3, 'ExportTierIndex': 3},
                                                       {'StartTime': {'Hour': 20, 'Minute': 0, 'Second': 0},
                                                        'ImportTierIndex': 1, 'ExportTierIndex': 1}]}]}},
                'PrepaymentConfig': {"CurrencyCode": 36, "InitialCreditAmount": 100, "MaxCreditLimit": 30000,
                                     "MaxVendLimit": 10000, "LowCreditAlarmThreshold": 125, "EmergencyCreditLimit": 50,
                                     "EmergencyCreditThreshold": 50, "LowEmergencyCreditAlarmThreshold": 40,
                                     "CutOffValue": -1, "CutOffGracePeriod": 1, "LoadLimitBelowCutOff": 10,
                                     "FCCalendar": {"Name": "F Calend", "TimeReference": 0,
                                                    "Days": [{"FCSwitches": [{"StartTime": {"Hour": 0, "Minute": 0,
                                                                                            "Second": 0},
                                                                              "Active": False}]}]},
                                     "FriendlyCreditWarning": 25, "InterruptSuspendTime": 10,
                                     "HaltStandingChargeOnSelfDisconnection": False, "HaltStandingChargeInEC": False},
                'BillingPeriod': {'Enabled': True, 'Interval': {'Duration': 2, 'DurationUnit': 2}}
            }

            logger.info(f'Request Data: {payload}')

            response = post_request('ChangeTariffPlan', payload)
            if response.status_code in [200, 202]:
                get_task_reply(response.text)
            else:
                logger.info(STATUS_CODE.get(response.status_code))

        # set PaymentCard id
        set_payment_card_id(tp_number, iteration, spn, meter)

    except Exception as e:
        exception_log(e)


@duration
def reset_counters(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {reset_counters.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = json.loads(data_from_xl['CounterList'])
        payload = sp.reset_counters_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')
        dict_to_update['CounterList'] = [{"CounterType": item, "Value": 0} for item in dict_to_update['CounterList']]
        dict_to_update['CounterList'] = [add_prefix_in_key(item, item.get('CounterType')) for item in
                                         dict_to_update['CounterList']]

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ResetCounters', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_meter_diagnostic_data(tp_number, iteration, spn, meter, 'Counters',
                                      [list(item.keys())[0] for item in dict_to_update])
        else:
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_meter_diagnostic_data(tp_number, iteration, spn: str, meter: str, dict_name: str | None = None,
                              kw_filter: list = None, col_no: int | None = 3):
    try:
        logger.info(f'Service called: {get_meter_diagnostic_data.__name__}')

        # get payload for service
        payload = sp.get_meter_diagnostic_data_payload(spn, meter)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        response = post_request('GetMeterDiagnosticData', payload)
        if response.status_code in [200, 202]:
            status = get_meter_diagnostic_data_reply(tp_number, iteration, response.text, dict_name, kw_filter, col_no)
            return status
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_meter_diagnostic_data_reply(tp_number, iteration, wse_id, dict_name: str | None = None, kw_filter: list = None,
                                    col_no: int | None = 3):
    try:
        logger.info(f'Service called: {get_meter_diagnostic_data_reply.__name__}')

        response = get_request('GetMeterDiagnosticDataReply', wse_id)
        response_dict = json.loads(response.text)
        logger.info(f'Received Data: {response.text}')

        temp_dict = copy.deepcopy(response_dict)

        new_dict = dict()
        key = None
        match dict_name:
            case 'RestrictDataOnDisplay':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'MeterData']
                temp_dict['MeterData']['RestrictDataOnDisplay'] = \
                    temp_dict['MeterData']['IsDataRestrictedOnDisplay']
                del temp_dict['MeterData']['IsDataRestrictedOnDisplay']

            case 'PaymentCardId':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'MeterData']

            case 'Counters':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'MeterData']
                temp_dict['MeterData']['Counters'] = \
                    [add_prefix_in_key(item, item.get('CounterType')) for item in temp_dict['MeterData']
                    ['Counters']]

            case 'CurrentTime':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'MeterData']

        temp_dict['MeterData']['MeterTime'] = convert_timestamp(temp_dict['MeterData']['MeterTime'])
        temp_dict['MeterData']['TimeZone'] = temp_dict['MeterData']['TimeZoneOffset']
        del temp_dict['MeterData']['TimeZoneOffset']

        new_dict.update({element: temp_dict[element] for element in key if temp_dict.get(element, '') != ''})

        time_difference = datetime.timedelta(hours=10, minutes=00)
        new_dict['MeterData']['MeterTime'] = datetime.datetime.strptime(new_dict['MeterData']['MeterTime'],
                                                                        '%d/%m/%Y %H:%M:%S') + time_difference
        new_dict['MeterData']['MeterTime'] = new_dict['MeterData']['MeterTime'].strftime('%d/%m/%Y %H:%M:%S')

        filtered_dict = dict_filter(new_dict, kw_filter)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, col_no)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, col_no - 2, col_no,
                        col_no + 2)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, col_no - 1, col_no + 1,
                        col_no + 3)

        # print(f'Received data is compared with the requested data.)er
        logger.info(f'Received data is compared with the requested data.')
        return response_dict

    except Exception as e:
        exception_log(e)


@duration
def adjust_meter_time(tp_number, iteration, spn, meter, time_in_seconds):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {adjust_meter_time.__name__}')

        # get payload for service
        payload = sp.adjust_meter_time_payload(spn, meter, time_in_seconds)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        response = post_request('AdjustMeterTime', payload)
        if response.status_code in [200, 202]:
            status = get_task_reply(response.text)
            return status

        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_snapshot(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str,
                 dict_name: str | None = None, kw_filter: list = None):
    try:
        logger.info(f'Service called: {get_snapshot.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['SnapshotType', 'StartTime', 'EndTime']
        data_from_xl = {x: data_from_xl[x] for x in keys}
        data_from_xl['StartTime'] = rf"/Date({time_in_seconds(data_from_xl['StartTime'])})/"
        data_from_xl['EndTime'] = rf"/Date({time_in_seconds(data_from_xl['EndTime'])})/"

        # get payload for service
        payload = sp.get_snapshot_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        response = post_request('GetSnapshot', payload)
        if response.status_code in [200, 202]:
            get_snapshot_reply(tp_number, iteration, response.text, dict_name, kw_filter, spn, meter)
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_snapshot_reply(tp_number, iteration, wse_id, dict_name: str | None = None, kw_filter: list = None, 
                       spn: str | None = None, meter: str | None = None):
    try:
        logger.info(f'Service called: {get_snapshot_reply.__name__}')

        response = get_request('GetSnapshotReply', wse_id)
        response_dict = json.loads(response.text)

        logger.info(f'Received Data: {response.text}')

        filtered_dict = dict_filter(response_dict, kw_filter)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.)
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def change_dst_configuration(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_dst_configuration.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = data_from_xl['DstConfig']
        payload = sp.change_dst_configuration_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')
        response = post_request('ChangeDstConfiguration', payload)
        dict_to_update['DstConfig'][0]['StartTime'] = convert_timestamp(dict_to_update['DstConfig'][0]['StartTime'])
        dict_to_update['DstConfig'][0]['EndTime'] = convert_timestamp(dict_to_update['DstConfig'][0]['EndTime'])

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            get_dst_configuration(tp_number, iteration, spn, meter, [list(item.keys())[0] for item in dict_to_update])
        else:
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_dst_configuration(tp_number, iteration, spn, meter, kw_filter: list = None):
    try:
        # print(f'Service called: {get_schedule.__name__})
        logger.info(f'Service called: {get_dst_configuration.__name__}')

        response = post_request('GetDstConfiguration', sp.get_dst_configuration_payload(spn, meter))
        get_dst_configuration_reply(tp_number, iteration, int(response.text), kw_filter)

    except Exception as e:
        exception_log(e)


def get_dst_configuration_reply(tp_number, iteration, wse_id, kw_filter: list = None):
    try:
        # print(f'Service called: {get_schedule_reply.__name__})
        logger.info(f'Service called: {get_dst_configuration_reply.__name__}')

        response = get_request('GetDstConfigurationReply', wse_id)
        response_dict = json.loads(response.text)

        # print(f'Received Data: {response.text}')
        logger.info(f'Received Data: {response.text}')
        # response_dict['DstConfig'] = \
        #     [add_prefix_in_key(item, item.get('EventCode')) for item in response_dict['EventConfig']]

        filtered_dict = dict_filter(response_dict, kw_filter)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        # print(f'Received data is compared with the requested data.)
        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def publish_change_of_tenancy(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        logger.info(f'Service called: {publish_change_of_tenancy.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        data_from_xl = json.loads(data_from_xl['ActionControl'])
        payload = sp.publish_change_of_tenancy_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('PublishChangeOfTenancy', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            header = em.read_rows(r".\Database\GetMeterEvents.xls", 1, 1)[0]
            data_from_xl = em.read_rows(r".\Database\GetMeterEvents.xls", 2, 2)[0]
            get_meter_events(tp_number, iteration, header, data_from_xl, spn, meter, 'CotCosSnapshots',
                             [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


@duration
def publish_change_of_supplier(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        logger.info(f'Service called: {publish_change_of_supplier.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['SupplierCode', 'ActionControl']
        data_from_xl = {x: data_from_xl[x] for x in keys}

        data_from_xl['ActionControl'] = json.loads(data_from_xl['ActionControl'])
        payload = sp.publish_change_of_supplier_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('PublishChangeOfSupplier', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            header = em.read_rows(r".\Database\GetMeterEvents.xls", 1, 1)[0]
            data_from_xl = em.read_rows(r".\Database\GetMeterEvents.xls", 2, 2)[0]
            get_meter_events(tp_number, iteration, header, data_from_xl, spn, meter, 'CotCosSnapshots',
                             [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_meter_events(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str,
                     dict_name: str | None = None, kw_filter: list = None):
    try:
        logger.info(f'Service called: {get_meter_events.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['EventLogList', 'StartTime', 'EndTime']
        data_from_xl = {x: data_from_xl[x] for x in keys}
        data_from_xl['StartTime'] = rf"/Date({time_in_seconds(data_from_xl['StartTime'])})/"
        data_from_xl['EndTime'] = rf"/Date({time_in_seconds(data_from_xl['EndTime'])})/"

        # get payload for service
        payload = sp.get_meter_events_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        response = post_request('GetMeterEvents', payload)
        if response.status_code in [200, 202]:
            get_meter_events_reply(tp_number, iteration, response.text, dict_name, kw_filter, spn, meter)
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_meter_events_reply(tp_number, iteration, wse_id, dict_name: str | None = None, kw_filter: list = None,
                           spn: str | None = None, meter: str | None = None):
    try:
        logger.info(f'Service called: {get_meter_events_reply.__name__}')

        response = get_request('GetMeterEventsReply', wse_id)
        response_dict = json.loads(response.text)

        logger.info(f'Received Data: {response.text}')

        new_dict = dict()
        key = None

        match dict_name:
            case 'CotCosSnapshots':
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'CotCosSnapshots']

            case 'debt':
                account_details = get_account_details(spn, meter)
                account_details['DebtAccountInfo'] = [add_prefix_in_key(item, item.get('DebtAccountType'))
                                                      for item in account_details['DebtAccountInfo']]

                # for item in account_details['DebtAccountInfo']:
                flattened_debt_info = []
                for item in account_details['DebtAccountInfo']:
                    flattened_debt_info.extend(key_rename(item))
                response_dict.update({'DebtAccountInfo': flattened_debt_info})
                response_dict.update({'PrepayAccountSnapshots': response_dict['PrepayAccountSnapshots'][-1]})
                response_dict['DebtChangeSnapshots'] = [add_prefix_in_key(item, item.get('DebtAccountType'))
                                                        for item in response_dict['DebtChangeSnapshots']]
                response_dict.update({'DebtChangeSnapshots': response_dict['DebtChangeSnapshots'][-1]})
                key = ['SupplyType', 'ServicePointNo', 'DeviceNo', 'PrepayAccountSnapshots', 'DebtChangeSnapshots',
                       'DebtAccountInfo']

        new_dict.update({element: response_dict[element] for element in key if response_dict.get(element, '') != ''})

        filtered_dict = dict_filter(new_dict, kw_filter)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, filtered_dict, 3)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 1, 3, 5)
        em.compare_data(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, 2, 4, 6)

        logger.info(f'Received data is compared with the requested data.')

    except Exception as e:
        exception_log(e)


@duration
def set_meter_time(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['TimeStamp', 'TimeZone']
        data_from_xl = {x: data_from_xl[x] for x in keys}
        data_from_xl['TimeZone'] = int(float(data_from_xl['TimeZone']))

        time_zone = data_from_xl['TimeZone']
        payload = sp.set_meter_time_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        logger.info(f'{meter}: Reading meter current time.')
        meter_dt_tm = read_meter_date_time(tp_number, iteration, spn, meter, dict_to_update)

        if meter_dt_tm is False:
            logger.info(f'{meter}: Unable to read meter date time')
            sys.exit()

        logger.info(f'{meter}: Meter current time is 'f'{meter_dt_tm + datetime.timedelta(minutes=(time_zone / 60))}.')

        if data_from_xl['TimeStamp'] == '':
            current_dt_tm = datetime.datetime.now(datetime.timezone.utc)
        else:
            current_dt_tm = datetime.datetime.strptime(str(data_from_xl['TimeStamp']), '%d/%m/%Y %H:%M:%S') - \
                            datetime.timedelta(minutes=(time_zone / 60))
            current_dt_tm = current_dt_tm.replace(tzinfo=datetime.timezone.utc)
        logger.info(f'{meter}: Time to set in meter is: '
                    f'{current_dt_tm + datetime.timedelta(minutes=(time_zone / 60))}.')

        # get the difference between meter date time and calculated date time
        time_diff = current_dt_tm - meter_dt_tm + datetime.timedelta(minutes=(time_zone / 60))

        # convert difference into seconds
        time_diff = time_diff.total_seconds() - time_zone
        logger.info(f'{meter}: Total time difference in seconds is "{time_diff}".')

        # set the date time in the meter
        logger.info(f'{meter}: Setting the date time to require date time.')
        status = adjust_meter_time(tp_number, iteration, spn, meter, int(time_diff))
        get_meter_diagnostic_data(tp_number, iteration, spn, meter, 'CurrentTime',
                                  [list(item.keys())[0] for item in dict_to_update])

        if status is False:
            logger.info(f'{meter}: Meter {meter} date time could not be set.')

        else:
            logger.info(f'{meter}: Meter {meter} date time '
                        f'{current_dt_tm + datetime.timedelta(minutes=(time_zone / 60))} set successfully')
            sys.exit()

    except Exception as e:
        exception_log(e)


def read_meter_date_time(tp_number, iteration, spn, meter, dict_to_update):
    try:
        data_in_json = get_meter_diagnostic_data(tp_number, iteration, spn, meter, 'CurrentTime',
                                                 [list(item.keys())[0] for item in dict_to_update])
        secs = data_in_json['MeterData']['MeterTime']
        secs = secs[6:secs.rfind(')')]
        secs = int(secs) / 1000
        dt_tm = datetime.datetime.fromtimestamp(secs, datetime.UTC)
        return dt_tm
    except Exception as e:
        exception_log(e)
        return False


@duration
def change_debt_configuration(tp_number, iteration, header: str, data_from_xl: list, spn: str, meter: str):
    try:
        # print(f'Service called: {set_schedule.__name__})
        logger.info(f'Service called: {change_debt_configuration.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        keys = ['DebtAccountType', 'DebtActionType', 'OperationMode', 'DebtAmount', 'DebtRecoveryInfo']
        data_from_xl = {x: data_from_xl[x] for x in keys}
        
        if 'DebtRecoveryInfo' in data_from_xl:
            data_from_xl['DebtRecoveryInfo'] = json.loads(data_from_xl['DebtRecoveryInfo'])

        payload = sp.change_debt_configuration_payload(spn, meter, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')
        dict_to_update.pop('DebtAmount')
        keys_list = ['DebtAccountType', 'DebtActionType', 'DebtRecoveryInfo']
        debt_list = []
        for key in keys_list:
            debt_list.append({key: dict_to_update[key]})
            dict_to_update.pop(key)
        dict_to_update.update({'debt_list': debt_list})
        dict_to_update['debt_list'] = [add_prefix_in_key(item, dict_to_update['debt_list'][0]['DebtAccountType'])
                                       for item in dict_to_update['debt_list']]
        dict_to_update['debt_list'][-1] = key_rename(dict_to_update['debt_list'][-1])

        em.list_of_dict.clear()  # clear old data if there is any
        dict_to_update = em.get_all_keys_values(dict_to_update)

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeDebtConfiguration', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            header = em.read_rows(r".\Database\GetMeterEvents.xls", 1, 1)[0]
            data_from_xl = em.read_rows(r".\Database\GetMeterEvents.xls", 2, 2)[0]
            get_meter_events(tp_number, iteration, header, data_from_xl, spn, meter, 'debt',
                             [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_account_details(spn, meter):
    try:
        # print(f'Service called: {get_schedule.__name__})
        logger.info(f'Service called: {get_account_details.__name__}')

        response = post_request('GetAccountDetails', sp.get_account_details_payload(spn, meter))
        details = get_account_details_reply(int(response.text))
        return details

    except Exception as e:
        exception_log(e)


def get_account_details_reply(wse_id):
    try:
        # print(f'Service called: {get_schedule_reply.__name__})
        logger.info(f'Service called: {get_account_details_reply.__name__}')

        response = get_request('GetAccountDetailsReply', wse_id)
        response_dict = json.loads(response.text)

        # print(f'Received Data: {response.text}')
        logger.info(f'Received Data: {response.text}')
        return response_dict

    except Exception as e:
        exception_log(e)

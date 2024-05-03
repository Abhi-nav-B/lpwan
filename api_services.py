import json
import requests
import logging
import service_payload as sp
import excel_manangement as em
from requests.auth import HTTPBasicAuth
from constants import *
from common_fn import exception_log, countdown, duration, xl_data_to_list, dict_filter

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
        payload = sp.set_schedule_payload(meter, spn, data_from_xl)

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
            get_schedule(tp_number, iteration, meter, spn)
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_schedule(tp_number, iteration, meter, spn):
    try:
        # print(f'Service called: {get_schedule.__name__})
        logger.info(f'Service called: {get_schedule.__name__}')

        response = post_request('GetSchedule', sp.get_schedule_payload(meter, spn))
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
        # print(f'Service called: {get_Supply_Control.__name__}')
        logger.info(f'Service called: {get_supply_control.__name__}')

        # converting excel data to a list
        header = xl_data_to_list(header)
        data_from_xl = xl_data_to_list(data_from_xl)

        # converting header and data_from_list to dict
        data_from_xl = dict(zip(header, data_from_xl))

        # get payload for service
        payload = sp.get_supply_control_payload(meter, spn, data_from_xl)

        # print(f'Request Data: {payload}')
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
        # print(f'Service called: {get_supply_control_code_reply.__name__}')
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

        # print(f'Received data is compared with the requested data.')
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
        payload = sp.change_price_payload(meter, spn, data_from_xl)

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

            get_meter_configuration(tp_number, iteration, meter, spn, 'ChangePrice',
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
        payload = sp.change_prepayment_configuration_payload(meter, spn, data_from_xl)

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
            get_meter_configuration(tp_number, iteration, meter, spn, 'PrepaymentConfig',
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

        payload = sp.change_billing_dates_payload(meter, spn, data_from_xl)

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
            get_meter_configuration(tp_number, iteration, meter, spn, 'BillingPeriod',
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
        data_from_xl['PrepaymentConfig'] = json.loads(data_from_xl['PrepaymentConfig'])
        data_from_xl['BillingPeriod'] = json.loads(data_from_xl['BillingPeriod'])

        payload = sp.change_tariff_plan_payload(meter, spn, data_from_xl)

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
            get_meter_configuration(tp_number, iteration, meter, spn, 'TariffPlan',
                                    [list(item.keys())[0] for item in dict_to_update])
        else:
            # print(STATUS_CODE.get(response.status_code))
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_meter_configuration(tp_number, iteration, meter: str, spn: str, dict_name: str | None = None,
                            kw_filter: list = None):
    try:
        # print(f'Service called: {get_Supply_Control.__name__})
        logger.info(f'Service called: {get_meter_configuration.__name__}')

        # get payload for service
        payload = sp.get_meter_configuration_payload(meter, spn, False)

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

        # print(f'Received Data: {response.text}')
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
                response_dict["TariffScheme"]["Import"]["Tou"]["ImportPrices"] = response_dict["TariffScheme"]["Import"]["Tou"]["TierPrices"]
                del response_dict["TariffScheme"]["Import"]["Tou"]["TierPrices"]
                response_dict["TariffScheme"]["Export"]["Tou"]["ExportPrices"] = response_dict["TariffScheme"]["Export"]["Tou"]["TierPrices"]
                del response_dict["TariffScheme"]["Export"]["Tou"]["TierPrices"]

        for element in key:
            new_dict.update({element: response_dict[element]})

        filtered_dict = dict_filter(new_dict, kw_filter)
        # filtered_dict['LoadLimitBelowCutOff'] = float(filtered_dict['LoadLimitBelowCutOff'])

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
        data_from_xl = json.loads(data_from_xl['EventConfig'])
        payload = sp.change_event_configuration_payload(meter, spn, data_from_xl)

        # print(f'Request Data: {payload}')
        logger.info(f'Request Data: {payload}')

        # remove not required dict items
        dict_to_update = payload.copy()
        dict_to_update.pop('HesId')
        dict_to_update.pop('Priority')

        em.write_in_xl(rf'{RESULT_PATH}\{tp_number}\{tp_number}_Service.xlsx', iteration, dict_to_update, 1)

        response = post_request('ChangeEventConfiguration', payload)
        if response.status_code in [200, 202]:
            get_task_reply(response.text)
            em.list_of_dict.clear()
            dict_to_update = em.get_all_keys_values(dict_to_update)
            get_event_configuration(tp_number, iteration, meter, spn, [list(item.keys())[0] for item in dict_to_update])
        else:
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_event_configuration(tp_number, iteration, meter, spn, kw_filter: list = None):
    try:
        # print(f'Service called: {get_schedule.__name__})
        logger.info(f'Service called: {get_event_configuration.__name__}')

        response = post_request('GetEventConfiguration', sp.get_event_configuration_payload(meter, spn))
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
        payload = sp.change_profile_configuration_payload(meter, spn, data_from_xl)

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
            get_profile_configuration(tp_number, iteration, meter, spn, [list(item.keys())[0] for item in dict_to_update])
        else:
            logger.info(STATUS_CODE.get(response.status_code))

    except Exception as e:
        exception_log(e)


def get_profile_configuration(tp_number, iteration, meter, spn, kw_filter: list = None):
    try:
        # print(f'Service called: {get_schedule.__name__})
        logger.info(f'Service called: {get_profile_configuration.__name__}')

        response = post_request('GetProfileConfiguration', sp.get_profile_configuration_payload(meter, spn))
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

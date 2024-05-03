from common_fn import get_hes_id
from common_fn import time_in_seconds
import json


def default_payload(meter: str, spn: str):
    payload = {
        'HesId': get_hes_id(),
        'SupplyType': 0,
        'ServicePointNo': spn,
        'DeviceNo': meter,
        'Priority': 1,
    }
    return payload


def set_schedule_payload(meter: str, spn: str, payload_data: str) -> dict:
    payload = default_payload(meter, spn)
    payload.update({'ScheduledTasks': json.loads(payload_data.replace('\\n', ''))})

    # 'ScheduledTasks': json.loads(str(payload_data[9])[6:-1].replace('\\n', ''))
    return payload


def get_schedule_payload(meter: str, spn: str) -> dict:
    return default_payload(meter, spn)


def get_supply_control_payload(spn: str, meter: str, payload_data: dict):
    payload = default_payload(spn, meter)

    payload.update({'AutoTransfer': payload_data['AutoTransfer']})
    if payload_data['ActivationTime'] != '':
        payload.update({'ActivationTime': payload_data['ActivationTime']})
    payload.update({'IntendedSupplyState': int(float(payload_data['IntendedSupplyState']))})
    return payload


def change_price_payload(meter: str, spn: str, payload_data: dict) -> dict:
    payload = default_payload(meter, spn)

    payload_data['StandingCharge'] = float(payload_data['StandingCharge'])
    payload_data['ImportPrices'] = [float(item) for item in payload_data['ImportPrices']]
    payload_data['ExportPrices'] = [float(item) for item in payload_data['ExportPrices']]
    payload_data['TaxConfig'] = {key: float(value) for key, value in payload_data['TaxConfig'].items()}
    payload.update({'PriceConfig': payload_data})

    # if payload_data['ActivationTime'] != '':
    #     payload.update({'ActivationTime': payload_data['ActivationTime']})

    return payload


def get_meter_configuration_payload(meter: str, spn: str, is_delayed: bool) -> dict:
    payload = default_payload(meter, spn)

    if is_delayed:
        payload.update({'MeterConfigurationType': 1})
    else:
        payload.update({'MeterConfigurationType': 0})

    return payload


def change_prepayment_configuration_payload(meter: str, spn: str, payload_data: dict) -> dict:
    payload = default_payload(meter, spn)

    payload_data['LoadLimitBelowCutOff'] = float(payload_data['LoadLimitBelowCutOff'])
    payload.update({'PrepaymentConfig': payload_data})
    return payload


def change_billing_dates_payload(meter: str, spn: str, payload_data: dict) -> dict:
    payload = default_payload(meter, spn)
    payload_data['Interval']['StartDate'] = rf"/Date({time_in_seconds(payload_data['Interval']['StartDate'])})/"
    payload.update({'BillingPeriod': payload_data})
    return payload


def change_tariff_plan_payload(meter: str, spn: str, payload_data: dict) -> dict:
    payload = default_payload(meter, spn)

    payload_data['TariffScheme']['StandingCharge'] = float(payload_data['TariffScheme']['StandingCharge'])
    payload_data['TariffScheme']['TaxConfig'] = {key: float(value) for key, value in
                                                 payload_data['TariffScheme']['TaxConfig'].items()}
    if payload_data['TariffScheme']['Import']['ChargingType'] == 0:
        payload_data['TariffScheme']['Import']['Block']['BlockThresholds'] = [float(item) for item in
                                                                              payload_data['TariffScheme']['Import'][
                                                                              'Block']['BlockThresholds']]
        payload_data['TariffScheme']['Import']['Block']['BlockPrices'] = [float(item) for item in
                                                                          payload_data['TariffScheme']['Import']
                                                                          ['Block']['BlockPrices']]
        payload_data['TariffScheme']['Import']['Block']['BlockPeriod']['StartDate'] = rf"/Date({time_in_seconds(
            payload_data['TariffScheme']['Import']['Block']['BlockPeriod']['StartDate'])})/"

    elif payload_data['TariffScheme']['Import']['ChargingType'] == 1:
        payload_data['TariffScheme']['Import']['Tou']['TierPrices'] = [float(item) for item in
                                                                       payload_data['TariffScheme']['Import']['Tou']
                                                                       ['TierPrices']]
        payload_data['TariffScheme']['Export']['Tou']['TierPrices'] = [float(item) for item in
                                                                       payload_data['TariffScheme']['Export']['Tou']
                                                                       ['TierPrices']]
    payload_data['PrepaymentConfig']['LoadLimitBelowCutOff'] = (
            float(payload_data['PrepaymentConfig']['LoadLimitBelowCutOff']))

    payload.update({'TariffScheme': payload_data['TariffScheme']})
    payload.update({'PrepaymentConfig': payload_data['PrepaymentConfig']})
    payload.update({'BillingPeriod': payload_data['BillingPeriod']})
    payload_data['BillingPeriod']['Interval'][
        'StartDate'] = rf"/Date({time_in_seconds(payload_data['BillingPeriod']['Interval']['StartDate'])})/"
    return payload


def change_event_configuration_payload(meter: str, spn: str, payload_data: dict) -> dict:
    payload = default_payload(meter, spn)
    payload.update({'EventConfig': payload_data})
    return payload


def get_event_configuration_payload(meter: str, spn: str) -> dict:
    return default_payload(meter, spn)


def change_profile_configuration_payload(meter: str, spn: str, payload_data: dict) -> dict:
    payload = default_payload(meter, spn)

    payload.update({'StandardProfile': payload_data['StandardProfile']})
    if 'DiagnosticProfile' in payload_data:
        payload.update({'DiagnosticProfile': payload_data['DiagnosticProfile']})
    return payload


def get_profile_configuration_payload(meter: str, spn: str) -> dict:
    return default_payload(meter, spn)

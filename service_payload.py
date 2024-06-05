from common_fn import get_hes_id
from common_fn import time_in_seconds
import json


def default_payload(spn: str, meter: str):
    payload = {
        'HesId': get_hes_id(),
        'SupplyType': 0,
        'ServicePointNo': spn,
        'DeviceNo': meter,
        'Priority': 1,
    }
    return payload


def set_schedule_payload(spn: str, meter: str, payload_data: str) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'ScheduledTasks': json.loads(payload_data.replace('\\n', ''))})

    # 'ScheduledTasks': json.loads(str(payload_data[9])[6:-1].replace('\\n', ''))
    return payload


def get_schedule_payload(spn: str, meter: str) -> dict:
    return default_payload(spn, meter)


def get_supply_control_payload(spn: str, meter: str, payload_data: dict):
    payload = default_payload(spn, meter)

    payload.update({'AutoTransfer': payload_data['AutoTransfer']})
    if payload_data['ActivationTime'] != '':
        payload.update({'ActivationTime': payload_data['ActivationTime']})
    payload.update({'IntendedSupplyState': int(float(payload_data['IntendedSupplyState']))})
    return payload


def change_price_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)

    payload_data['StandingCharge'] = float(payload_data['StandingCharge'])
    payload_data['ImportPrices'] = [float(item) for item in payload_data['ImportPrices']]
    payload_data['ExportPrices'] = [float(item) for item in payload_data['ExportPrices']]
    payload_data['TaxConfig'] = {key: float(value) for key, value in payload_data['TaxConfig'].items()}
    payload.update({'PriceConfig': payload_data})

    # if payload_data['ActivationTime'] != '':
    #     payload.update({'ActivationTime': payload_data['ActivationTime']})

    return payload


def initialise_meter_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)

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
    payload.update({'DisconnectionAllowed': bool(payload_data['DisconnectionAllowed'])})
    payload.update({'TariffScheme': payload_data['TariffScheme']})
    if 'PrepaymentConfig' in payload_data:
        payload_data['PrepaymentConfig']['LoadLimitBelowCutOff'] = (
            float(payload_data['PrepaymentConfig']['LoadLimitBelowCutOff']))
        payload.update({'PrepaymentConfig': payload_data['PrepaymentConfig']})
    payload.update({'BillingPeriod': payload_data['BillingPeriod']})
    payload_data['BillingPeriod']['Interval'][
        'StartDate'] = rf"/Date({time_in_seconds(payload_data['BillingPeriod']['Interval']['StartDate'])})/"
    if 'GasConfig' in payload_data:
        payload_data['GasConfig']['ConversionFactor'] = (
            float(payload_data['GasConfig']['ConversionFactor']))
        payload_data['GasConfig']['CalorificValue'] = (
            float(payload_data['GasConfig']['CalorificValue']))
        payload.update({'GasConfig': payload_data['GasConfig']})
    if 'ImportCO2Config' in payload_data:
        payload_data['ImportCO2Config']['CO2Factor'] = (
            float(payload_data['ImportCO2Config']['CO2Factor']))
        payload.update({'ImportCO2Config': payload_data['ImportCO2Config']})
    if 'ExportCO2Config' in payload_data:
        payload_data['ExportCO2Config']['CO2Factor'] = (
            float(payload_data['ExportCO2Config']['CO2Factor']))
        payload.update({'ExportCO2Config': payload_data['ExportCO2Config']})
    return payload


def change_prepayment_configuration_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)

    payload_data['LoadLimitBelowCutOff'] = float(payload_data['LoadLimitBelowCutOff'])
    payload.update({'PrepaymentConfig': payload_data})
    return payload


def change_gas_parameters_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)

    payload_data['ConversionFactor'] = float(payload_data['ConversionFactor'])
    payload_data['CalorificValue'] = float(payload_data['CalorificValue'])
    payload.update({'GasConfig': payload_data})
    return payload


def change_co2_configuration_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)

    if 'ImportCO2Config' in payload_data:
        payload_data['ImportCO2Config']['CO2Factor'] = (
            float(payload_data['ImportCO2Config']['CO2Factor']))
        payload.update({'ImportCO2Config': payload_data['ImportCO2Config']})
    if 'ExportCO2Config' in payload_data:
        payload_data['ExportCO2Config']['CO2Factor'] = (
            float(payload_data['ExportCO2Config']['CO2Factor']))
        payload.update({'ExportCO2Config': payload_data['ExportCO2Config']})
    return payload


def get_meter_configuration_payload(spn: str, meter: str, is_delayed: bool) -> dict:
    payload = default_payload(spn, meter)

    if is_delayed:
        payload.update({'MeterConfigurationType': 1})
    else:
        payload.update({'MeterConfigurationType': 0})

    return payload


def change_billing_dates_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)
    payload_data['Interval']['StartDate'] = rf"/Date({time_in_seconds(payload_data['Interval']['StartDate'])})/"
    payload.update({'BillingPeriod': payload_data})
    return payload


def change_disconnection_setting_payload(spn: str, meter: str, payload_data: bool) -> dict:
    payload = default_payload(spn, meter)
    if payload_data:
        payload.update({'DisconnectionAllowed': True})
    else:
        payload.update({'DisconnectionAllowed': False})
    return payload


def change_tariff_plan_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)

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
    if 'StartDate' in payload_data['BillingPeriod']['Interval']:
        payload_data['BillingPeriod']['Interval'][
            'StartDate'] = rf"/Date({time_in_seconds(payload_data['BillingPeriod']['Interval']['StartDate'])})/"
    return payload


def change_event_configuration_payload(spn: str, meter: str, payload_data: str) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'EventConfig': json.loads(payload_data.replace('\\n', ''))})
    return payload


def get_event_configuration_payload(spn: str, meter: str) -> dict:
    return default_payload(spn, meter)


def change_profile_configuration_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)

    payload.update({'StandardProfile': payload_data['StandardProfile']})
    if 'DiagnosticProfile' in payload_data:
        payload.update({'DiagnosticProfile': payload_data['DiagnosticProfile']})
    return payload


def get_profile_configuration_payload(spn: str, meter: str) -> dict:
    return default_payload(spn, meter)


def change_system_parameters_payload(spn: str, meter: str, payload_data: dict) -> tuple:
    payload = default_payload(spn, meter)
    payload_list = list(payload_data['ParamSettings'].keys())
    payload_list.sort()
    payload_data_result = {i: payload_data['ParamSettings'][i] for i in payload_list}
    payload.update({'ParamSettings': payload_data_result})
    return payload, payload_list


def get_system_parameters_payload(spn: str, meter: str) -> dict:
    return default_payload(spn, meter)


def change_demand_limit_configuration_payload(spn: str, meter: str, payload_data: str) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'DemandLimitConfig': json.loads(payload_data)})

    return payload


def get_demand_limit_configuration_payload(spn: str, meter: str) -> dict:
    return default_payload(spn, meter)


def change_data_disclosure_settings_payload(spn: str, meter: str, payload_data: bool) -> dict:
    payload = default_payload(spn, meter)
    if payload_data:
        payload.update({'RestrictDataOnDisplay': True})
    else:
        payload.update({'RestrictDataOnDisplay': False})
    return payload


def reset_counters_payload(spn: str, meter: str, payload_data: str) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'CounterList': payload_data})
    return payload


def get_meter_diagnostic_data_payload(spn: str, meter: str) -> dict:
    return default_payload(spn, meter)


def set_payment_card_id_payload(spn: str, meter: str, payload_data: str) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'PaymentCardId': payload_data})
    return payload


def adjust_meter_time_payload(spn: str, meter: str, payload_data: int) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'TimeCorrection': payload_data})
    return payload


def set_meter_time_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'MeterTime': payload_data['TimeStamp']})
    payload.update({'TimeZone': payload_data['TimeZone']})
    return payload


def get_snapshot_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'SnapshotType': payload_data['SnapshotType']})
    payload.update({'StartTime': payload_data['StartTime']})
    payload.update({'EndTime': payload_data['EndTime']})
    return payload


def change_dst_configuration_payload(spn: str, meter: str, payload_data: str) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'DstConfig': json.loads(payload_data)})
    payload['DstConfig'][0]['StartTime'] = rf"/Date({time_in_seconds(payload['DstConfig'][0]['StartTime'])})/"
    payload['DstConfig'][0]['EndTime'] = rf"/Date({time_in_seconds(payload['DstConfig'][0]['EndTime'])})/"
    return payload


def get_dst_configuration_payload(spn: str, meter: str) -> dict:
    return default_payload(spn, meter)


def publish_change_of_tenancy_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)

    payload.update({'ActionControl': payload_data})
    return payload


def get_meter_events_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'EventLogList': json.loads(payload_data['EventLogList'])})
    payload.update({'StartTime': payload_data['StartTime']})
    payload.update({'EndTime': payload_data['EndTime']})
    return payload


def publish_change_of_supplier_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'SupplierCode': payload_data['SupplierCode']})
    payload.update({'ActionControl': payload_data['ActionControl']})
    return payload


def get_site_information_payload(spn: str) -> dict:
    payload = {
        'HesId': get_hes_id(),
        'SupplyType': 0,
        'ServicePointNo': spn
    }
    return payload


def get_payment_card_information_payload(payload_data: str) -> dict:
    payload = {
        'HesId': get_hes_id(),
        'PaymentCardId': payload_data
    }
    return payload


def deregister_payment_card_payload(payload_data: str) -> dict:
    payload = {
        'HesId': get_hes_id(),
        'PaymentCardId': payload_data
    }
    return payload


def register_payment_card_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)
    payload.update({'PaymentCardId': payload_data['PaymentCardId']})
    payload.update({'CustomerName': payload_data['CustomerName']})
    payload.update({'VatGroupNo': int(payload_data['VatGroupNo'])})
    return payload


def change_debt_configuration_payload(spn: str, meter: str, payload_data: dict) -> dict:
    payload = default_payload(spn, meter)

    payload.update({'DebtAccountType': int(float(payload_data['DebtAccountType']))})
    payload.update({'DebtActionType': int(float(payload_data['DebtActionType']))})
    if 'OperationMode' in payload_data:
        payload.update({'OperationMode': int(float(payload_data['OperationMode']))})
    if 'DebtAmount' in payload_data:
        payload.update({'DebtAmount': int(float(payload_data['DebtAmount']))})
    if 'DebtRecoveryInfo' in payload_data:
        payload.update({'DebtRecoveryInfo': payload_data['DebtRecoveryInfo']})
    return payload


def get_account_details_payload(spn: str, meter: str) -> dict:
    return default_payload(spn, meter)

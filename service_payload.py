from common_fn import get_hes_id, time_in_seconds
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


def set_schedule_payload(spn: str, meter: str,  payload_data: str) -> dict:
    payload = default_payload(spn,  meter)
    payload.update({'ScheduledTasks': json.loads(payload_data.replace('\\n', ''))})

    # 'ScheduledTasks': json.loads(str(payload_data[9])[6:-1].replace('\\n', ''))
    return payload


def get_schedule_payload(spn: str, meter: str) -> dict:
    return default_payload(spn, meter)


def get_Supply_Control_payload(spn: str, meter: str,  payload_data: dict):
    payload = default_payload(spn, meter)

    payload.update({'AutoTransfer': payload_data['AutoTransfer']})
    if payload_data['ActivationTime'] != '':
        payload.update({'ActivationTime': payload_data['ActivationTime']})
    payload.update({'IntendedSupplyState': int(float(payload_data['IntendedSupplyState']))})
    return payload


def change_billing_dates_payload(meter: str, spn: str, payload_data: dict) -> dict:
    payload = default_payload(meter, spn)
    payload_data['Interval']['StartDate'] = rf"/Date({time_in_seconds(payload_data['Interval']['StartDate'])})/"
    payload.update({'BillingPeriod': payload_data})
    return payload

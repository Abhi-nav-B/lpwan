import excel_management as em
import api_services as service
import logging
from common_fn import start_logger, exception_log, duration
from constants import RESULT_PATH

logger = logging.getLogger('my_logger')


@duration
def execute_test(tp_number: str, service_name: str, test_data_path: str,
                 iteration_from: int, iteration_to: int, spn: str, meter: str, gateway: str):
    start_logger(rf'{RESULT_PATH}\{tp_number}\{tp_number}.log')

    try:
        # get excel data
        header = em.read_rows(test_data_path, 1, 1)[0]
        for iteration, data_from_xl in enumerate(em.read_rows(test_data_path, iteration_from, iteration_to)):
            logger.info(f'{"":-<80}')
            logger.info(f'{"Execution started":-^80}')
            logger.info(f'Execution of test {tp_number} is started for test data row number '
                        f'{iteration_from + iteration}.')

            match service_name:
                case 'adjust_meter_time':
                    service.adjust_meter_time(tp_number, iteration_from + iteration, header, data_from_xl, spn, meter)

                case 'initialise_meter':
                    service.initialise_meter(tp_number, iteration_from + iteration, header, data_from_xl, spn, meter)

                case 'change_tariff_plan':
                    service.change_tariff_plan(tp_number, iteration_from + iteration, header, data_from_xl, spn, meter)

                case 'change_price':
                    service.change_price(tp_number, iteration_from + iteration, header, data_from_xl, spn, meter)

                case 'change_prepayment_configuration':
                    service.change_prepayment_configuration(tp_number, iteration_from + iteration,
                                                            header, data_from_xl, spn, meter)

                case 'change_billing_dates':
                    service.change_billing_dates(tp_number, iteration_from + iteration,
                                                 header, data_from_xl, spn, meter)

                case 'change_co2_configuration':
                    service.change_co2_configuration(tp_number, iteration_from + iteration, header, data_from_xl, spn,
                                                     meter)

                case 'change_currency':
                    pass

                case 'change_profile_configuration':
                    service.change_profile_configuration(tp_number, iteration_from + iteration, header, data_from_xl,
                                                         spn, meter)

                case 'change_event_configuration':
                    service.change_event_configuration(tp_number, iteration_from + iteration, header, data_from_xl, spn,
                                                       meter)

                case 'change_dst_configuration':
                    service.change_dst_configuration(tp_number, iteration_from + iteration, header, data_from_xl, spn,
                                                     meter)

                case 'change_system_parameters':
                    service.change_system_parameters(tp_number, iteration_from + iteration, header, data_from_xl, spn,
                                                     meter)

                case 'update_device_service_point':
                    pass

                case 'set_schedule':
                    service.set_schedule(tp_number, iteration_from + iteration, header, data_from_xl, spn, meter)

                case 'get_Supply_Control':
                    service.get_supply_control(tp_number, iteration_from + iteration, header, data_from_xl, spn, meter)

                case 'send_text_message':
                    service.send_text_message(tp_number, iteration_from + iteration, header, data_from_xl, spn, meter)

                case 'change_gas_parameters':
                    service.change_gas_parameters(tp_number, iteration_from + iteration, header, data_from_xl, spn,
                                                  meter)

                case 'change_demand_limit_configuration':
                    service.change_demand_limit_configuration(tp_number, iteration_from + iteration, header,
                                                              data_from_xl, spn, meter)

                case 'change_disconnection_setting':
                    service.change_disconnection_setting(tp_number, iteration_from + iteration, spn, meter)

                case 'change_data_disclosure_settings':
                    service.change_data_disclosure_settings(tp_number, iteration_from + iteration, spn, meter)

                case 'set_payment_card_id':
                    service.set_payment_card_id(tp_number, iteration_from + iteration, header, data_from_xl, spn,
                                                meter)

                case 'reset_counters':
                    service.reset_counters(tp_number, iteration_from + iteration, header, data_from_xl, spn, meter)

                case 'publish_change_of_tenancy':
                    service.publish_change_of_tenancy(tp_number, iteration_from + iteration, header, data_from_xl, spn,
                                                      meter)

                case 'publish_change_of_supplier':
                    service.publish_change_of_supplier(tp_number, iteration_from + iteration, header, data_from_xl, spn,
                                                       meter)
                
                case 'set_meter_time':
                    service.set_meter_time(tp_number, iteration_from + iteration, header, data_from_xl, spn, meter)
                
                case 'set_and_register_payment_card_id':
                    service.set_and_register_payment_card_id(tp_number, iteration_from + iteration, header,
                                                             data_from_xl, spn, meter)
                case 'change_debt_configuration':
                    service.change_debt_configuration(tp_number, iteration_from + iteration, header, data_from_xl, spn,
                                                      meter)
                    
            logger.info(f'{"Execution completed":-^80}')
            logger.info(f'{"":_<80}')

    except Exception as e:
        exception_log(e)


def call_service(tp_number: str, service_name: str, spn: str, meter: str, gateway: str, payload: dict):
    match service_name:
        case 'get_Supply_Control':
            service.get_supply_control(tp_number, service_name, spn, meter, payload)

METER = 'SAM000012597'
GATEWAY = 'HHRND00021'
SERVICE_POINT = METER


testcase_list = [
    # {'tp_number': 'TPxxxxx-0001-C1',
    #  'service_name': 'set_schedule',
    #  'test_data_path': r".\Database\SetSchedule.xls",
    #  'iteration_from': 2,
    #  'iteration_to': 5,
    #  'spn': SERVICE_POINT,
    #  'meter': METER,
    #  'gateway': GATEWAY,
    #  },

    # {'tp_number': 'TPxxxxx-0002-C1',
    #  'service_name': 'get_Supply_Control',
    #  'test_data_path': r".\Database\GetSupplyControlCode.xls",
    #  'iteration_from': 2,
    #  'iteration_to': 2,
    #  'spn': SERVICE_POINT,
    #  'meter': METER,
    #  'gateway': GATEWAY,
    #  },

    # {'tp_number': 'TPxxxxx-0003-C1',
    #  'service_name': 'set_schedule',
    #  'test_data_path': r".\Database\SetSchedule.xls",
    #  'iteration_from': 4,
    #  'iteration_to': 7,
    #  'spn': SERVICE_POINT,
    #  'meter': METER,
    #  'gateway': GATEWAY,
    #  },

    # {'tp_number': 'TPxxxxx-0004-C1',
    #  'service_name': 'get_Supply_Control',
    #  'test_data_path': r".\Database\GetSupplyControlCode.xls",
    #  'iteration_from': 4,
    #  'iteration_to': 6,
    #  'spn': SERVICE_POINT,
    #  'meter': METER,
    #  'gateway': GATEWAY,
    #  },

    {'tp_number': 'TPxxxxx-0007-C1',
     'service_name': 'change_billing_dates',
     'test_data_path': r".\Database\ChangeBillingDates.xls",
     'iteration_from': 2,
     'iteration_to': 2,
     'spn': SERVICE_POINT,
     'meter': METER,
     'gateway': GATEWAY,
     },
]

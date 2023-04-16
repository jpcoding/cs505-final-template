import time

from PayloadGen import getpayload, get_zip
from Publisher import pub
import json

# generate initial data
zip_map = get_zip()


# uncomment to create local test files

patient_list, hospital_list, vax_list = getpayload(zip_map, 4)

text_file = open("patient_data.json", "w")
n = text_file.write(patient_list)
text_file.close()

text_file = open("hospital_data.json", "w")
n = text_file.write(hospital_list)
text_file.close()

text_file = open("vax_data.json", "w")
n = text_file.write(vax_list)
text_file.close()

# patient_list_1 = '[\n    {\n        "testing_id": 2,\n        "patient_name": "Lillie Grimes",\n        "patient_mrn": "85aa0394-dbb3-11ed-90f6-3af4def4deda",\n        "patient_zipcode": "42539",\n        "patient_status": "1",\n        "contact_list": [\n            "85aa0394-dbb3-11ed-90f6-3af4def4deda"\n        ],\n        "event_list": [\n            "85aa0312-dbb3-11ed-90f6-3af4def4deda"\n        ]\n    }\n]'
# patient_list_2 = '[\n    {\n        "testing_id": 2,\n        "patient_name": "Lillie Grimes",\n        "patient_mrn": "85aa0394-dbb3-11ed-90f6-3af4def4deda",\n        "patient_zipcode": "42539",\n        "patient_status": "1",\n        "contact_list": [\n            "85aa0394-dbb3-11ed-90f6-3af4def4deda"\n        ],\n        "event_list": [\n            "85aa0312-dbb3-11ed-90f6-3af4def4deda"\n        ]\n    }, {\n        "testing_id": 2,\n        "patient_name": "Lillie Grimes",\n        "patient_mrn": "85aa0394-dbb3-11ed-90f6-3af4def4deda",\n        "patient_zipcode": "42539",\n        "patient_status": "1",\n        "contact_list": [\n            "85aa0394-dbb3-11ed-90f6-3af4def4deda"\n        ],\n        "event_list": [\n            "85aa0312-dbb3-11ed-90f6-3af4def4deda"\n        ]\n    }\n]'

# patient_list = [patient_list_1, patient_list_2]

p1 = {
        "testing_id": 6,
        "patient_name": "Isreal Melugin",
        "patient_mrn": "b7dd59a0-dbb4-11ed-8688-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "b7dd59b4-dbb4-11ed-8688-3af4def4deda",
            "b7dd59a0-dbb4-11ed-8688-3af4def4deda"
        ],
        "event_list": [
            "b7dd58ba-dbb4-11ed-8688-3af4def4deda"
        ]
    }


patient_list_list2 = json.dumps([
    {
        "testing_id": 6,
        "patient_name": "Isreal Melugin",
        "patient_mrn": "b7dd59a0-dbb4-11ed-8688-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "b7dd59b4-dbb4-11ed-8688-3af4def4deda",
            "b7dd59a0-dbb4-11ed-8688-3af4def4deda"
        ],
        "event_list": [
            "b7dd58ba-dbb4-11ed-8688-3af4def4deda"
        ]
    },
    {
        "testing_id": 8,
        "patient_name": "Beverly Holliday",
        "patient_mrn": "b7dd59b4-dbb4-11ed-8688-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "b7dd59a0-dbb4-11ed-8688-3af4def4deda",
            "b7dd59a0-dbb4-11ed-8688-3af4def4deda"
        ],
        "event_list": [
            "b7dd58ba-dbb4-11ed-8688-3af4def4deda"
        ]
    }
])

patient_list_list1 =json.dumps([
    {
        "testing_id": 5,
        "patient_name": "Tony Kezar",
        "patient_mrn": "8105aed2-dbb4-11ed-90f6-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "8105aee6-dbb4-11ed-90f6-3af4def4deda"
        ],
        "event_list": [
            "8105aeb4-dbb4-11ed-90f6-3af4def4deda",
            "8105ae64-dbb4-11ed-90f6-3af4def4deda"
        ]
    },
    {
        "testing_id": 4,
        "patient_name": "Antonio Blanton",
        "patient_mrn": "8105aee6-dbb4-11ed-90f6-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "8105af40-dbb4-11ed-90f6-3af4def4deda",
            "8105aed2-dbb4-11ed-90f6-3af4def4deda"
        ],
        "event_list": [
            "8105adce-dbb4-11ed-90f6-3af4def4deda",
            "8105ae96-dbb4-11ed-90f6-3af4def4deda",
            "8105ae46-dbb4-11ed-90f6-3af4def4deda"
        ]
    },
    {
        "testing_id": 2,
        "patient_name": "Judy Dudziak",
        "patient_mrn": "8105aefa-dbb4-11ed-90f6-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "8105af54-dbb4-11ed-90f6-3af4def4deda",
            "8105aed2-dbb4-11ed-90f6-3af4def4deda",
            "8105aed2-dbb4-11ed-90f6-3af4def4deda"
        ],
        "event_list": [
            "8105ae96-dbb4-11ed-90f6-3af4def4deda"
        ]
    },
    {
        "testing_id": 10,
        "patient_name": "Cassandra Bailey",
        "patient_mrn": "8105af18-dbb4-11ed-90f6-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "8105af2c-dbb4-11ed-90f6-3af4def4deda",
            "8105af54-dbb4-11ed-90f6-3af4def4deda"
        ],
        "event_list": [
            "8105ae96-dbb4-11ed-90f6-3af4def4deda",
            "8105ae46-dbb4-11ed-90f6-3af4def4deda",
            "8105ae96-dbb4-11ed-90f6-3af4def4deda"
        ]
    },
    {
        "testing_id": 1,
        "patient_name": "Floyd Seals",
        "patient_mrn": "8105af2c-dbb4-11ed-90f6-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "8105aefa-dbb4-11ed-90f6-3af4def4deda",
            "8105aee6-dbb4-11ed-90f6-3af4def4deda"
        ],
        "event_list": [
            "8105aeb4-dbb4-11ed-90f6-3af4def4deda",
            "8105adce-dbb4-11ed-90f6-3af4def4deda"
        ]
    },
    {
        "testing_id": 10,
        "patient_name": "Sammie Wyatt",
        "patient_mrn": "8105af40-dbb4-11ed-90f6-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "8105aed2-dbb4-11ed-90f6-3af4def4deda",
            "8105af40-dbb4-11ed-90f6-3af4def4deda",
            "8105af18-dbb4-11ed-90f6-3af4def4deda",
            "8105aefa-dbb4-11ed-90f6-3af4def4deda",
            "8105af54-dbb4-11ed-90f6-3af4def4deda"
        ],
        "event_list": [
            "8105ae96-dbb4-11ed-90f6-3af4def4deda",
            "8105ae64-dbb4-11ed-90f6-3af4def4deda",
            "8105adce-dbb4-11ed-90f6-3af4def4deda",
            "8105aeb4-dbb4-11ed-90f6-3af4def4deda",
            "8105aeb4-dbb4-11ed-90f6-3af4def4deda"
        ]
    },
    {
        "testing_id": 4,
        "patient_name": "Elliott Goodman",
        "patient_mrn": "8105af54-dbb4-11ed-90f6-3af4def4deda",
        "patient_zipcode": "11111",
        "patient_status": "1",
        "contact_list": [
            "8105aee6-dbb4-11ed-90f6-3af4def4deda",
            "8105af54-dbb4-11ed-90f6-3af4def4deda"
        ],
        "event_list": [
            "8105adce-dbb4-11ed-90f6-3af4def4deda",
            "8105ae82-dbb4-11ed-90f6-3af4def4deda"
        ]
    }
])


f = open('patient2.json')
patient_list_list1 = json.dumps(json.load(f))
f.close()
f = open('patient7.json')
patient_list_list2 = json.dumps(json.load(f))
f.close()
patient_list = [patient_list_list1, patient_list_list2]


while True:

    # patient_list, hospital_list, vax_list = patient_list, hospital_list, vax_list

    for x in range(1,8):
        pub(str(1), patient_list[x%2], 'patient_list')
        pub(str(1), hospital_list, 'hospital_list')
        pub(str(1), vax_list, 'vax_list')

    time.sleep(15)


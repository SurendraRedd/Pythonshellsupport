# File name: main.py
# Author: Surendra Reddy
# Date created: 7/6/2021
# Date last modified: 7/6/2021

import json
import os,sys

def clean(data):
    if isinstance(data, str):
        return data.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ').replace('"', ' ').replace('\\', '\\\\').replace("u'","'").replace('\\n', '').replace('\\t', '').replace('\\r', '').replace('\\', '').replace('~','')
    if isinstance(data, list):
        return [clean(item) for item in data]
    if isinstance(data, dict):
        return {clean(key): clean(value) for (key, value) in data.items()}
    return data

def parseJson():
    # with open('C:/Users/surredd/Downloads/daily_dump_dcpp_2021_06_22_16_58_53.json') as file:
    #     value = json.load(file)
    #     #print(len(value["data"]))
    #     for i in range(0, len(value["data"])):
    #         print(f"Device Counts value is {value['data'][i]['DEVICE_COUNTS']}")
    #         print(f"Cabinet Count value is {value['data'][i]['CABINET_COUNT']}")
    #     file.close()

    attributes = 'DEPLOYMENT_ID,FORECAST_ID,CST_TICKET,CST_TITLE,PROPERTY_POC,' \
                 'PROPERTY_POC_DS_ID,DATE_ADDED,BUSINESS_ORG,PROPERTY,APPLICATION,' \
                 'PROJECT_NAME,LOCATION,IS_POC,DEPLOYMENT_TYPE,CUSTOMER_PRIORITY,C19_CORE_PRIORITY,' \
                 'BUSINESS_JUSTIFICATION,IN_QUEUE_STATUS,ESCALATION_STATUS,PRIORITY_SET_DATE,PRIORITY_SETTER,' \
                 'IC_BUILD_NEEDED,COMPLETED,COMPLETED_DATE,STATUS_NOTES,IS_POC_DS_ID,TOMAHAWK_YES_NO,ESCALATED_BY,' \
                 'ESCALATED_BY_DS_ID,PRIORITY_SETTER_DS_ID,EST_CUST_HAND_OFF_DATE,DEPLOYMENT_STATUS,LAST_MODIFIED_BY,' \
                 'LAST_MODIFIED_TIMESTAMP,PROCESSED,PRIORITY_NOTES,INTERNAL_NOTES,CUSTOMER_VIS_NOTES,RELATED_RECORDS,' \
                 'CATALOG_TASKS,TAGS,ACTION,CST_STATE,CST_STAGE,CST_OPENED_BY,CST_OPENED_BY_DS_ID,P0_P1_JUSTIFICATION,' \
                 'P0_P1_DIRECTOR_EMAIL,ESCALATED_DATE,UPDATED_BY_TYPE,OLD_NOTES,ITRAC_INC,SS_TASK_CLOSED_DATE,' \
                 'CST_LATEST_COMMENTS,DERIVED_HANDOVER_DATE,CHECKPOINTS_JSON,CHECKPOINTS_DATE_JSON,SAFE_ID,IC_LOCATION,' \
                 'ADBD_MATCH,LAST_MODIFIED_BY_SYSTEM,LAST_MODIFIED_SYS_TIMESTAMP,DEVICE_COUNTS,CABINET_COUNT'

    # Set output files
    output_file = open('./gdcs_deployment.txt', "w")
    try:
        # Provide the file path
        r = open('C:/Users/surredd/Downloads/daily_dump_dcpp_2021_06_22_16_58_53.json', 'r')
        rf = r.read()
        # print("print rf data",rf
        global json_data
        json_data = json.loads(rf)
        if json_data["data"] is None:
            sys.exit(1)
    except Exception as E1:
        print(E1)

    for list in json_data and json_data["data"]:
        row = []
        for col in attributes.split(','):
            if col in list and list[col]:
                # if col == 'DEVICE_COUNTS' or col == 'CABINET_COUNT':
                #     print('found values')
                row.append(clean(str(list[col])))
            else:
                row.append("")
        output_file.write('~'.join(row) + "\n")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parseJson()
    print('👍 Completed Parsing')

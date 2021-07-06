import csv
import datetime
import zipfile
import os

toCSV = [{'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'agentCurrentCPUUtilization', 'val': 0}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'agentFreeMemory', 'val': 29507196}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'agentTotalMemory', 'val': 32218820}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.45.192.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.45.192.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.47.0.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.47.0.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.51.224.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.51.224.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.69.224.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.69.224.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.70.224.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.70.224.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.71.96.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.10.71.96.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.108.32.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.108.32.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.108.96.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.108.96.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.110.128.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.110.128.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.110.192.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.110.192.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.112.96.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.112.96.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.113.32.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.113.32.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.113.96.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.113.96.1', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.114.96.0', 'val': 1}, {'ts': '1624300560', 'time_stamp': datetime.datetime(2021, 6, 21, 18, 36, tzinfo=datetime.timezone.utc), 'host': 'cnsha7-wlc-2.asia.apple.com', 'metric': 'bsnAPIfAdminStatus.8.79.169.29.114.96.1', 'val': 1}]
keyValue = toCSV[0].keys()
hostname = toCSV[0]['host'].split('.')[0]
if os.path.isfile('./output.csv'):
    print('Output csv file exists, hence appending the data')
    with open('output.csv', 'a', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keyValue)
        dict_writer.writeheader()
        dict_writer.writerows(toCSV)
        output_file.close()
else:
    print('output.csv file does not exists, hence it will be created newly')
    with open('output.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keyValue)
        dict_writer.writeheader()
        dict_writer.writerows(toCSV)
        output_file.close()

'''
# Time being commented this code for gzip files creation
list_files = [hostname+'-output.csv']

with zipfile.ZipFile("file.zip", "w") as zipF:
    for file in list_files:
        zipF.write(file, compress_type=zipfile.ZIP_DEFLATED)
'''
import gzip
import io
keyValue = toCSV[0].keys()
hostname = toCSV[0]['host'].split('.')[0]

file_name = hostname + "tesfile.csv.gz"
for result in toCSV:
    epic_record = ','.join(str(x) for x in result.values())
    with gzip.open(file_name,'ab') as output:
        with io.TextIOWrapper(output, encoding='utf-8') as encode:
            encode.write(epic_record + '\n')

# a_file = gzip.open(hostname + "tesfile.csv.gz", "rb")
# contents = a_file.read()

# print(contents)

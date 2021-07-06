# read_epic_daily.py
import csv
import gzip
import datetime
#from datetime import date,datetime,timedelta
import json
import os
import sys
from pprint import pprint
import glob
import pandas as pd
import requests
import recertifi
import re
import requests
import csv
import configparser
import collections
import codecs
import logging
import logging.config
import importlib
importlib.reload(sys)
import warnings
warnings.filterwarnings("ignore")
from retrying import retry
import time
import io
from io import StringIO
import string
import shutil
counter=0
# ===========================================================================================================#
# Function Name: log_info
# Purpose: Standardize Log Messages
# Input Parameters: 1 string input - message to be printed in Logfile
# Return Parantere: None
# ===========================================================================================================#
def log_info(logmsg):
  #print >> fh, date.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S") + " : Info : %s" % (logmsg)
  print(datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S") + " : Info : %s"%(logmsg), file = fh)
  fh.flush()
# ===========================================================================================================#
# Function Name: get_env_variables
# Purpose: Set Global Variables, and import environment and config parameters
# Input Parameters: None
# Return Parantere: None
# ===========================================================================================================#
def get_env_variables():
  global CONFIG_DIR
  CONFIG_DIR = os.environ["CONFIG_DIR"]
  cconfigparser = configparser.RawConfigParser()
  configFilePath = CONFIG_DIR + '/epic_capri_accesspoints_api.conf'
  cconfigparser.read(configFilePath)
  global INPUT_DIR
  INPUT_DIR = os.environ["INPUT_DIR"]
  global OUTPUT_DIR
  OUTPUT_DIR = os.environ["OUTPUT_DIR"]
  global florence_input
  florence_input = os.environ["florence_input"]
  global capri_input
  capri_input = os.environ["capri_input"]
  global EPIC_URL
  EPIC_URL = os.environ["EPIC_URL"]
  global EPIC_NODE_URL
  EPIC_NODE_URL = os.environ["EPIC_NODE_URL"]
  #global v_output_file
  #v_output_file = cconfigparser.get('EPIC_HOST_DAILY', 'output_file')
  # global start_time
  # start_time=os.environ["starttime"]
  global LOGFILE
  LOGFILE = os.environ["LOGFILE"]
  global ETL_JOB_RUN_ID
  ETL_JOB_RUN_ID = os.environ["ETL_JOB_RUN_ID"]
  global DATE_TODAY
  DATE_TODAY = datetime.date.today().strftime('%Y-%m-%d')

@retry(stop_max_attempt_number=7,wait_fixed=2000)
def call_epic(hostname: str, locale, instance, start, end, force=True):
    """
    Get network traffic going in and out of all ports for a host during the
    specified time frame
    """
    results = []
    #print(f'Calling Epic : start = {start} & end = {end} for {hostname}',flush=True)
    start = int(start)
    end = int(end)
    try:
      params = {
        'a': 'json',
        'ds': '.*',
        'n': hostname,
        's': start,
        'e': end,
        'v': 4 # The version of the epic api to call
      }
      request_url = f"{EPIC_URL}{instance}@{locale}/{hostname}/"
      #msg=request_url
      #log_info("Calling-%s"%(msg))
      print(f'Calling {request_url}',flush=True)
      res = requests.get(request_url, params=params)
      res_data = res.json()
      if 'intervals' not in res_data:
        # Call did not return expected values
        sys.stderr.write('Failed to get network traffic data for'+ hostname + '\n')
        return []
      for ts in res_data['intervals']:
        for i, stat in enumerate(res_data['meta']['ds']['name']):
          time_stamp = datetime.datetime.utcfromtimestamp(int(ts)).replace(tzinfo=datetime.timezone.utc)
          res_dict = {
            'ts': ts,
            'time_stamp': time_stamp,
            'host': hostname,
            'metric': stat,
            'val': res_data['intervals'][ts][i]
          }
          results.append(res_dict)
    except KeyError:
      file_name = f"{INPUT_DIR}/FAILED_{hostname}.csv"
      return (file_name, None)
    log_info(type(results))
    #log_info(results)
    return results

@retry(stop_max_attempt_number=7,wait_fixed=2000)
def get_locale_instance(node: str):
  """
  Get locale and instance for a given WLC
  """
  params = {
    'search_q': node,
  }
  res = requests.get(EPIC_NODE_URL, params=params)
  res_dict = res.json()
  if 'meta' not in res_dict or len(res_dict['objects']) == 0:
    raise ValueError("Can't get node info from Epic for " + node)
  data = res_dict['objects'][0]
  return data['locale'], data['instance']

def get_results_wlc(wlc: str, start, end):
  """
  Send calls to Epic for a WLC, locale, and instance in the given time frame
  and write results to a CSV
  """
  hdrs = {
    'Content-Type': 'application/json'
  }
  # Get locale and instance
  try:
    locale, instance = get_locale_instance(wlc)
  except ValueError as E1:
    # Skip WLC if Epic can't find it
    log_info(E1)
    return
  # Call Epic
  msg={wlc,locale,instance,start,end}
  log_info("Calling Epic with locale and inst%s"%(msg))
  results = call_epic(wlc, locale, instance, start, end)
  # Write results to a CSV
  # TODO: Write to Parquet instead of CSV
  try:
      date = datetime.datetime.utcfromtimestamp(start).strftime('%m%d%Y')
      global counter
      counter=counter+1
      log_info("counter value is %s"%(counter))
      toCSV = results
      keyValue = toCSV[0].keys()
      file_name = f"{INPUT_DIR}/EPIC_{ETL_JOB_RUN_ID}_{counter}.csv"

      try:
        # Create the csv filename
        with open(file_name, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keyValue)
            dict_writer.writeheader()
            dict_writer.writerows(toCSV)

        # csv file names
        list_files = [file_name]
        try:
            with open(list_files[0], 'rb') as f_in:
                with gzip.open(file_name + ".gz", 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            try:
                # remove the csv files
                os.remove(list_files[0])
            except:
                log_info("Error while deleting file %s"%(list_files[0]))
        except FileNotFoundError:
          log_info("Unable to create the .gz file ...")
          return
      except FileNotFoundError:
          log_info("Error in writing CSV file ...")
          return
  except KeyError:
      return (None, None)

'''
  for result in results:
    epic_record = ','.join(str(x) for x in result.values())
    with gzip.open(file_name,'ab') as output:
       with io.TextIOWrapper(output, encoding='utf-8') as encode:
         encode.write(epic_record + '\n')
  toCSV = results
  keys = toCSV[0].keys()
  file_name = f"{INPUT_DIR}/EPIC_{ETL_JOB_RUN_ID}.csv.gz"
  file_name_csv = f"{INPUT_DIR}/EPIC_{ETL_JOB_RUN_ID}.csv"
  with open(file_name_csv, 'w', newline='') as output_file:
     dict_writer = csv.DictWriter(output_file, keys)
     dict_writer.writeheader()
     dict_writer.writerows(toCSV)
  #list_files = [file_name_csv]
  #with open(file_name_csv, 'rb') as f_in:
  #   with gzip.open(file_name, 'wb') as f_out:
  #     shutil.copyfileobj(f_in, f_out)
  file_content = open(file_name_csv,'r').read()
  for i in file_content:
    with open(file_name, 'ab') as output:
       with io.TextIOWrapper(output, encoding='utf-8') as encode:
         encode.write(i)
  with zipfile.ZipFile(file_name, "w") as zipF:
     for file in list_files:
       zipF.write(file, compress_type=zipfile.ZIP_DEFLATED)
  try:
    date = datetime.datetime.utcfromtimestamp(start).strftime('%m%d%Y')
    file_name = f"{INPUT_DIR}/EPIC_{ETL_JOB_RUN_ID}.ZIP"
    #log_info(file_name)
    hdrs = ['time_stamp', 'ts', 'host', 'metric', 'val']
    if os.path.exists(file_name): # Append to existing file
      with zipfile.ZipFile(file_name, mode='a', compression=zipfile.ZIP_DEFLATED) as zip_file:
         string_buffer = []
         string_buffer = StringIO()
         csvwrite = csv.DictWriter(string_buffer, delimiter=',', fieldnames=hdrs)
         for row in results:
           csvwrite.writerow(row)
         zip_file.writestr(file_name + '.csv', string_buffer.getvalue())
         zip_file.close()
      return (file_name, results)
    else: # Create file and write
      with zipfile.ZipFile(file_name, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
         string_buffer = []
         string_buffer = StringIO()
         csvwrite = csv.DictWriter(string_buffer, delimiter=',', fieldnames=hdrs)
         csvwrite.writeheader()
         for row in results:
           csvwrite.writerow(row)
         zip_file.writestr(file_name + '.csv', string_buffer.getvalue())
         zip_file.close()
      return (file_name, results)
  except KeyError:
      return (None, None)
'''
def get_epic_data():
  client_ap_df = pd.read_csv(florence_input,sep='|')
  #client_ap_df.query('state==Detected', inplace=True) #and 'deviceState==Active')
  #print(client_ap_df)
  # Shorten MAC address to 16 characters
  #print(client_ap_df.head())
  #print(client_ap_df.BSSID)
  client_ap_df['short_mac'] = client_ap_df['fixedBSSID'].str.slice(0,16)
  # AP/Controller lookup
  ap_lookup = pd.read_csv(capri_input,sep='|')
  ap_lookup = ap_lookup[['fqdn','dot3MacAddress']]
  ap_lookup['short_mac'] = ap_lookup['dot3MacAddress'].str.slice(0,16)
  client_ap_df = client_ap_df.merge(right=ap_lookup, how='left',on='short_mac')
  # Break down time into days
  start = min(client_ap_df['epochtime'])
  first_day = start - (start % 86400)
  end = max(client_ap_df['epochtime'])
  days = [d for d in range(first_day,end,86400)]
  # Get list of previously run days in output directory (if any)
  written_files = glob.glob(INPUT_DIR + "/epic_*.csv")
  if len(written_files)>0:
    written_days = [f.split(INPUT_DIR+'/epic_')[1].split('.csv')[0] for f in written_files]
  else:
    written_days=[]
  # Query epic for all controllers, each day
  for d in days:
    # Check for duplicate written files
    date = datetime.datetime.utcfromtimestamp(d).strftime('%m%d%Y')
    #print("date",date)
    #print("written_days",written_days)
    if date in written_days:
      continue
    d_events = client_ap_df[(client_ap_df['epochtime']>=d)&(client_ap_df['epochtime']<(d+86400))]
    #print(d_events)
    controllers = d_events['fqdn'].unique()
    controllers = [x for x in controllers if str(x) != 'nan']
    #print("controllers",controllers)
    #print(controllers)
    for wlc in controllers:
      #print("entered loop for")
      start = d
      end = d+86400-1
      try :
         msg={wlc,start,end}
         log_info("calling get_results_wlc -%s"%(msg))
         get_results_wlc(wlc, start, end)
      except Exception as E1:
         log_info(E1)

# =========================================================================================================================================#
# Function Name: main
# Purpose: Main Function acts as starting point for the script
# Input Parameters: None
# Return Parantere: None
# =========================================================================================================================================#
def main():
  global msg
  global fh
  try:
    msg = "Attempting to fetch Environment Variables"
    get_env_variables()
    fh = open(LOGFILE,'a+')
  except Exception as E1:
    log_info("Unable to onboard Environment Variables-%s"%(E1))
    sys.exit(1)
  try:
    msg = "Attempting to Open Logfile"
    log_info(msg)
    log_info("Starting the Epic Pull for Access Points")
    get_epic_data()
  except Exception as E1:
    log_info(E1)
    sys.exit("Error while " + msg)

if __name__ == '__main__':
    main()
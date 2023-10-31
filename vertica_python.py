import os, sys, pandas as pd, warnings
from datetime import date,datetime,timedelta
from vertica_python import connect
warnings.filterwarnings("ignore")
#===========================================================================================================#
# Function Name: log_info
# Purpose: Standardize Log Messages
# Input Parameters: 1 string input - message to be printed in Logfile
# Return Parantere: None
#===========================================================================================================#
def log_info(logmsg):
  print(
      date.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S") +
      f" : Info : {logmsg}",
      file=fh,
  )
#===========================================================================================================#
# Function Name: get_env_variables
# Purpose: Set Global Variables, and import environment and config parameters
# Input Parameters: None
# Return Parantere: None
#===========================================================================================================#
def get_env_variables():
  global INPUT_DIR
  INPUT_DIR = os.environ["INPUT_DIR"]
  global CONFIG_DIR
  CONFIG_DIR = os.environ["CONFIG_DIR"]
  global LOGFILE
  LOGFILE = os.environ["LOGFILE"]
  global VERTICA_HOST_IP
  VERTICA_HOST_IP = sys.argv[1]
  global VERTICA_PORT
  VERTICA_PORT = 5433
  global VERTICA_USER
  VERTICA_USER = os.environ["VERTICA_DCI_OWNER"]
  global VERTICA_PASSWORD
  VERTICA_PASSWORD = os.environ["VERTICA_DCI_OWNER_PASSWORD"]
  global VERTICA_DCI_SCHEMA
  VERTICA_DCI_SCHEMA = os.environ["VERTICA_DCI_SCHEMA"]
  global QRY
  QRY=os.environ["CONTACT_QRY"]
#=========================================================================================================================================#
# Function Name: main
# Purpose: Main Function acts as starting point for the script
# Input Parameters: None
# Return Parantere: None
#=========================================================================================================================================#
def main():
  global msg
  global fh
  try:
    msg = "Attempting to fetch Environment Variables"
    get_env_variables()
  except:
    print ("Unable to onboard Environment Variables" )
    sys.exit(1)
  try:
    msg = "Attempting to Open Logfile"
    fh = open(LOGFILE,'a+')
    log_info(msg)
    msg = "Attempting to connect to Vertica"
    log_info(msg)
    conn = connect(host=VERTICA_HOST_IP, port=VERTICA_PORT, user=VERTICA_USER, password=VERTICA_PASSWORD)
    vdb_cursor = conn.cursor()
    qry = QRY
    log_info(qry)
    msg = "Creating Dataframe from stage table"
    log_info(msg)
    df = pd.DataFrame(vdb_cursor.execute(qry).fetchall(),columns=['ID', 'OWNER_CONTACT', 'OWNER_CONTACT_EMAIL'])
    msg = "Creating new Dataframe to consolidate contact details for assets"
    log_info(msg)
    df1 = df.groupby(['ID']).agg(','.join)
    msg = "Exporting Dataframe with consolidated contact details for assets into a file which can be loaded into Vertica using COPY commands"
    log_info(msg)
    df1.to_csv(f'{INPUT_DIR}/owner_contact_info.txt', sep='\t')
  except Exception as E1:
    log_info(E1)
    sys.exit(f"Error while {msg}")


if __name__ == '__main__':
    main()
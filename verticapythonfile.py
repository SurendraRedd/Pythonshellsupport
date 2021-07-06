from vertica_python import connect
import logging

conn_dict={
    'host' : '127.0.0.1',
    'port' : 5433,
    'user' : 'username',
    'password' : 'password',
    'database' : 'dbname',
    'log_level' : logging.INFO,
    'log_path' : './dbdetails.log'
}

def main():
    # Establish the connection
    with vertica_python.connect(**conn_dict) as connection:
        cur = connection.cursor()
        # Reading the csv file
        with open("filename.csv", "rb") as fs:
            csvFilename = fs.read().decode('utf-8', 'ignore')
            cur.execute("copy table_name FROM LOCAL '/Users/username/data.csv'  DELIMITER ',' EXCEPTIONS './load.log' REJECTED DATA './reject.log' direct ;", csvFilename)
            connection.commit()

if __name__ == '__main__':
    main()

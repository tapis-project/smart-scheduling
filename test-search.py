from __future__ import print_function
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode
import os
import re
import pytz
import logging
from typing import List
import socket
import math
import shutil
import time

# **************************************************************
# ASSIGN THESE RUNTIME PARAMETERS FOR YOUR ENVIRONMENT
#
# Before running this program assign the following parameters
# that are valid in your environment.

my_host = "localhost"
my_user = "root"
my_passwd = "password"
my_database = "HPC_Job_Time_Data"  # the default should be 'hpc'
my_parent_dir = "/home/rcardone/work/smartsched/hpc/"
# **************************************************************

# ---------------------------------------------------
# GLOBAL variables and constants.
total_errors = 0;
total_files_skipped = 0
filecount = 0

# Number of pipe-separated fields in raw input files
SHORT_RECORD_LEN = 13
QOS_RECORD_LEN = 14
# ---------------------------------------------------

def connect():

    connection = mysql.connector.connect(host=my_host, user=my_user, passwd=my_passwd, database=my_database)

    print('\nSuccessfully Connected to SQL Database:', my_database)

    return connection

def createIfNotExists(connection, tableName):

    '''

    :param connection: Connection parameter to the SQL database
    :param cursor: Cursor element that allows access to different tools in the SQL database
    :param tableName: User provided tableName
    :return: Returns print statement based off condition whether a table was created if it did not exist, as well as indices that would be created if the table did not exist

    '''

    connection.get_warnings = True
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (jobid varchar(30) NOT NULL PRIMARY KEY, user varchar(80) NOT NULL, account varchar(60) NOT NULL, start datetime NOT NULL, end datetime NOT NULL, submit datetime NOT NULL, queue varchar(30) NOT NULL, max_minutes int unsigned NOT NULL, jobname varchar(60) NOT NULL, state varchar(20) NOT NULL, nnodes int unsigned NOT NULL, reqcpus int unsigned NOT NULL, nodelist TEXT NOT NULL, qos varchar(20))")
    tuple = cursor.fetchwarnings() # <- returns a list of tuples

    if tuple is None:
        print('New table generated')
        # ALl permissions granted, no warning message or message in general outputted to the user, no need for conditional statements
        dbspec = my_database + '.' + tableName
        
        cursor.execute('CREATE INDEX index_jobid ON ' + dbspec + '(jobid)')
        cursor.execute('CREATE INDEX index_user ON ' + dbspec + '(user)')
        cursor.execute('CREATE INDEX index_account ON ' + dbspec + '(account)')

        cursor.execute('CREATE INDEX index_submit ON ' + dbspec + '(submit)')
        cursor.execute('CREATE INDEX index_start ON ' + dbspec + '(start)')
        cursor.execute('CREATE INDEX index_end ON ' + dbspec + '(end)')

        cursor.execute('CREATE INDEX index_queue ON ' + dbspec + '(queue)')
        cursor.execute('CREATE INDEX index_max_minutes ON ' + dbspec + '(max_minutes)')
        cursor.execute('CREATE INDEX index_state ON ' + dbspec + '(state)')

        cursor.execute('CREATE INDEX index_nnodes ON ' + dbspec + '(nnodes)')
        cursor.execute('CREATE INDEX index_reqcpus ON ' + dbspec + '(reqcpus)')
        #cursor.execute('CREATE INDEX index_nodelist ON ' + dbspec + '(nodelist)')
        cursor.execute('CREATE INDEX index_qos ON ' + dbspec + '(qos)')

        connection.commit() # Commits any tables and indices that were created to the database
        print('Indexes created\n')
    elif(tuple[0][1] == 1050):
        print('Table already exists\n')

    cursor.close()

def injection(connection, tableName):

    # Assign the actual directory that contains all the input files.
    source = my_parent_dir + tableName

    os.chdir(source)

    local = pytz.timezone("US/Central")
    global filecount
    global total_errors
    global total_files_skipped

    start = datetime.now()
    print('Start: ', start)
    for filename in sorted(os.listdir(source)):
        
        # Skip directories.
        if not os.path.isfile(filename):
            continue
            
        # Open a new cursor in existing connection.
        cursor = connection.cursor()
        
        # Read the first line of the file to determine the record format.
        readIn = open(filename, 'r')
        firstline = next(readIn)
        
        # Establish this file's record size.
        record_size = len(firstline.split('|'))
        if record_size != SHORT_RECORD_LEN and record_size != QOS_RECORD_LEN:
            total_files_skipped += 1
            print("\nERROR: Records with", record_size, "fields are not supported, skipping file", filename)
            continue
        
        # Read the rest of the file line by line.
        lineno = 1
        for line in readIn:
            # Assign line number
            lineno += 1
            
            # Parse next line and validate number of fields.
            row = line.split('|')
            size = len(row)
            if size != record_size: 
                total_errors += 1
                print("\nERROR: Record has", size, "fields, expected", record_size, "fields in", filename, "line", lineno)
                continue
            
            # Assign fields from left to right.
            jobid = str(row[0])
            user = str(row[1])
            account = str(row[2])

            # Generates a pytz.AmbiguousTimeError

            # Conversion to UTC for start, end and submit variables
            local_start = datetime.strptime(row[3], '%Y-%m-%dT%H:%M:%S')
            local_dt_strt = local.localize(local_start, is_dst=True)
            start = local_dt_strt.astimezone(pytz.utc)

            local_end = datetime.strptime(row[4], '%Y-%m-%dT%H:%M:%S')
            local_dt_end = local.localize(local_end, is_dst=True)
            end = local_dt_end.astimezone(pytz.utc)

            local_submit = datetime.strptime(row[5], '%Y-%m-%dT%H:%M:%S')
            local_dt_submit = local.localize(local_submit, is_dst=True)
            submit = local_dt_submit.astimezone(pytz.utc)

            queue = str(row[6])

            raw = str(row[7]) # Raw is the max_minutes column
            dash_position = raw.find('-')
            if (dash_position == 1):
                found = []
                if re.search('\:', raw) is not None:
                    for i in re.finditer('\:', raw):
                        found.append(i.start(0))
                if len(found) == 2:
                    #print('\nDays-Hours:Minutes:Seconds (D-H:M:S) Format')
                    #print(raw)

                    temp = re.split('[-]', raw)
                    day = int(temp[0]) * 1440  # The amount of minutes in a day

                    temp1 = temp[1]
                    hms = re.split('[:]', temp1)

                    h = int(hms[0]) * 60
                    m = int(hms[1])
                    s = int(hms[2]) * 0.0166667

                    max_minutes = day + h + m + s

                if len(found) == 1:
                    #print('\nDays-Hours:Minutes (D-H:M) Format')
                    #print(raw)
                    temp = re.split('[-]', raw)
                    day = int(temp[0]) * 1440  # The amount of minutes in a day

                    temp1 = temp[1]
                    hms = re.split('[:]', temp1)
                    h = int(hms[0]) * 60
                    m = int(hms[1])

                    max_minutes = day + h + m

                if re.search('\:', raw) is None:  # Working
                    #print('\nDay-Hour (DH) Format')
                    #print(raw)
                    temp = re.split('[-]', raw)
                    day = int(temp[0]) * 1440  # The amount of minutes in a day
                    h = int(temp[1]) * 60
                    max_minutes = day + h
            elif (dash_position == -1):  # .find() returns a -1 if the search case is not found
                found = []
                if re.search('\:', raw) is not None:
                    for i in re.finditer('\:', raw):
                        found.append(i.start(0))

                if len(found) == 2:
                    #print('\nHours:Minutes:Seconds (H:M:S) Format')
                    #print(raw)
                    hms = re.split('[:]', raw)
                    h = int(hms[0]) * 60
                    m = int(hms[1])
                    s = int(hms[2]) * 0.0166667

                    max_minutes = h + m + s

                if len(found) == 1:
                    #print('\nMinutes:Seconds (M:S) Format')
                    #print(raw)
                    hms = re.split('[:]', raw)
                    m = int(hms[0])
                    s = int(hms[1]) * 0.0166667

                    max_minutes = m + s


                if re.search('\:', raw) is None:  # Working
                    #print('\nMinute (MM) Format')
                    #print(raw)
                    max_minutes = int(raw)

            jobname = str(row[8])

            state = str(row[9])

            nnodes = intTryParse(row[10], filename, lineno)
            reqcpus = intTryParse(row[11], filename, lineno)
            nodelist = str(row[12])
            
            # Optional fields depending on record length.
            qos = None
            if record_size == QOS_RECORD_LEN:
                qos = str(row[13])

            add_data = ("INSERT IGNORE INTO " + tableName +
                        "(jobid, user, account, start, end, submit, queue, max_minutes, jobname, state, nnodes, reqcpus, nodelist, qos) "
                        "VALUES (%(jobid)s, %(user)s, %(account)s, %(start)s, %(end)s, %(submit)s, %(queue)s, %(max_minutes)s, %(jobname)s, %(state)s, %(nnodes)s, %(reqcpus)s, %(nodelist)s, %(qos)s)")

            data = {
                'jobid': jobid,
                'user': user,
                'account': account,
                'start': start,
                'end': end,
                'submit': submit,
                'queue': queue,
                'max_minutes': max_minutes,
                'jobname': jobname,
                'state': state,
                'nnodes': nnodes,
                'reqcpus': reqcpus,
                'nodelist': nodelist,
                'qos': qos,
            }

            cursor.execute(add_data, data)
        # Commit after writing all the data of the current file into the table
        connection.commit()
        
        # Print progress message over the last message without newline.
        filecount += 1
        progress = "Last file committed: " + str(filecount) + " - " + filename + "       "
        print(progress, end='\r')

        # Clean up.
        cursor.close()
        readIn.close()
        

    end = datetime.now()
    print('\nEnd: ', end)
   # rn = end - start
   # print('Total Run time: ', rn)
   
def intTryParse(value, filename, lineno):
    try:
        return int(value)
    except:
        global total_errors
        total_errors += 1
        print("\nERROR: Integer conversion in file ", filename, "line", lineno)
        return 0

def main():

    tableName = input('Enter a name of a HPC machine you would like to access: ')

    connection = connect()

    createIfNotExists(connection, tableName)
    injection(connection, tableName)
    
    print("Total record errors: ", total_errors)
    print("Total files skipped: ", total_files_skipped)
    print("Total files read: ", filecount)

    connection.close()

if __name__ == '__main__':
    main()

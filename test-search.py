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

def connect():

    connection = mysql.connector.connect(host="127.0.0.1", user='costaki', passwd='password', database="HPC_Job_Time_Data")

    print('\nSuccessfully Connected to SQL Database')

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

    cursor.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (jobid varchar(20) NOT NULL PRIMARY KEY, user varchar(80) NOT NULL, account varchar(50) NOT NULL, start datetime NOT NULL, end datetime NOT NULL, submit datetime NOT NULL, queue varchar(22) NOT NULL, max_minutes int unsigned NOT NULL, jobname varchar(50) NOT NULL, state varchar(20) NOT NULL, nnodes int unsigned NOT NULL, reqcpus int unsigned NOT NULL, nodelist TEXT NOT NULL)")
    tuple = cursor.fetchwarnings() # <- returns a list of tuples

    if tuple is None:
        print('New table generated\nCreating indexes')
        # ALl permissions granted, no warning message or message in general outputted to the user, no need for conditional statements

        cursor.execute('CREATE INDEX index_jobid ON HPC_Job_Time_Data.' + tableName + '(jobid)')
        cursor.execute('CREATE INDEX index_user ON HPC_Job_Time_Data.' + tableName + '(user)')
        cursor.execute('CREATE INDEX index_account ON HPC_Job_Time_Data.' + tableName + '(account)')

        cursor.execute('CREATE INDEX index_submit ON HPC_Job_Time_Data.' + tableName + '(submit)')
        cursor.execute('CREATE INDEX index_start ON HPC_Job_Time_Data.' + tableName + '(start)')
        cursor.execute('CREATE INDEX index_end ON HPC_Job_Time_Data.' + tableName + '(end)')

        cursor.execute('CREATE INDEX index_queue ON HPC_Job_Time_Data.' + tableName + '(queue)')
        cursor.execute('CREATE INDEX index_max_minutes ON HPC_Job_Time_Data.' + tableName + '(max_minutes)')
        cursor.execute('CREATE INDEX index_state ON HPC_Job_Time_Data.' + tableName + '(state)')

        cursor.execute('CREATE INDEX index_nnodes ON HPC_Job_Time_Data.' + tableName + '(nnodes)')
        cursor.execute('CREATE INDEX index_reqcpus ON HPC_Job_Time_Data.' + tableName + '(reqcpus)')
        #cursor.execute('CREATE INDEX index_nodelist ON HPC_Job_Time_Data.' + tableName + '(nodelist)')

        connection.commit() # Commits any tables and indices that were created to the database
        print('\nIndexes created')
    elif(tuple[0][1] == 1050):
        print('\nTable already exists')

    cursor.close()

def injection(connection, tableName):

    cursor = connection.cursor()

    source = '/home/ubuntu/jobs_data/' + tableName

    os.chdir(source)

    local = pytz.timezone("US/Central")
    counter = 0
    for filename in os.listdir(source):
        readIn = open(filename, 'r')
        next(readIn)
        for line in readIn:
            row = line.split('|')
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

            nnodes = int(row[10])
            reqcpus = int(row[11])
            nodelist = str(row[12])

            add_data = ("INSERT IGNORE INTO " + tableName +
                        "(jobid, user, account, start, end, submit, queue, max_minutes, jobname, state, nnodes, reqcpus, nodelist) "
                        "VALUES (%(jobid)s, %(user)s, %(account)s, %(start)s, %(end)s, %(submit)s, %(queue)s, %(max_minutes)s, %(jobname)s, %(state)s, %(nnodes)s, %(reqcpus)s, %(nodelist)s)")

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
                'jobname': jobname,
                'state': state,
                'nnodes': nnodes,
                'reqcpus': reqcpus,
                'nodelist': nodelist,
            }

            cursor.execute(add_data, data)
        counter += 1
        print(counter)
        # Commit after writing all the data of the current file into the table
        connection.commit()
    readIn.close()
    cursor.close()

def sort():

    source = '/home/ubuntu/jobs_data/Frontera'

    os.chdir(source)

    for filename in sorted(os.listdir(source)):
        readIn = open(filename, 'r')
        print('Current file being processed is: ', readIn)

def main():

    tableName = input('Enter a name of a HPC machine you would like to access: ')

    connection = connect()

    createIfNotExists(connection, tableName)
    injection(connection, tableName)

    connection.close()

if __name__ == '__main__':
    main()
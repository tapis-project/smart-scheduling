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
# These systematic information variables are necessary to run this script.
# Below are precreated instances, but they need to change based on a user-by-user basis due to information such as naming changing.
# Before running this program, assign the following parameters
# that are valid in your environment.
# For further instructions on what these variables mean, and how to update them for this program to run properly,
# please read the README.md in the Github repository this script was found.


my_host = "localhost"  # The host variable that the MySQL Database is created on (IE. IP address or local network)
my_user = "root"  # Connection instance username that has the ability to create and modify tables, indexes and databases
my_passwd = "password"  # Password for the user with the access mentioned on the line above
my_database = "HPC_Job_Time_Data"  # The MySQL variable that hosts the name of the database that the tables of the submitted data will be stored on (Variable name to change at discretion of user)
my_parent_dir = "/home/ubuntu/jobs_data/"  # The parent directory of the HPC-specific input directories that host the submitted job data that will be inserted into the MySQL table
# **************************************************************

# ---------------------------------------------------
# GLOBAL variables and constants.
total_errors = 0
total_files_skipped = 0
filecount = 0

# Number of pipe-separated fields in raw input files
SHORT_RECORD_LEN = 13
QOS_RECORD_LEN = 14
# ---------------------------------------------------

def connect():

    connection = mysql.connector.connect(host=my_host, user=my_user, passwd=my_passwd, database=my_database)

    print('\nSuccessfully Connected to your MySQL Database:', my_database)

    return connection

def connectGen():
    # This function is general connection to the MySQL connection instance to look at the available databases
    # Is used in createDatabase() function to create a database if it doesn't exist already
    genConnection = mysql.connector.connect(host=my_host, user=my_user, passwd=my_passwd)

    print('\nSuccessfully Connected to MySQL Workbench')
    cursor = genConnection.cursor()

    #print('\nList of available databases:')
    #cursor.execute('SHOW DATABASES')
    #for x in cursor:
    #    print(x)

    return genConnection

def createDatabase(genConnection, databaseName):

    if databaseName == "": # No user input, as such use the default database name from the above section
        print('Default database in use...')

    else: # Else, set the my_database variable to the new user inputted one, create if not exists by general cursor
        global my_database
        my_database = databaseName
        genConnection.get_warnings = True
        cursor = genConnection.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS " + databaseName)
        tuple = cursor.fetchwarnings()

        if tuple is None:
            cursor.execute("SHOW DATABASES")
            for x in cursor:
                print(x)
        elif(tuple[0][1] == 1007):
            print('\nDatabase already exists')

        cursor.close()

def createTable(connection, tableName):

    connection.get_warnings = True
    cursor = connection.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS " + tableName + " (jobid varchar(30) NOT NULL PRIMARY KEY, user varchar(80) NOT NULL, account varchar(60) NOT NULL, start datetime NOT NULL, end datetime NOT NULL, submit datetime NOT NULL, queue varchar(30) NOT NULL, max_minutes int unsigned NOT NULL, jobname varchar(60) NOT NULL, state varchar(20) NOT NULL, nnodes int unsigned NOT NULL, reqcpus int unsigned NOT NULL, nodelist TEXT NOT NULL, qos varchar(20))")
    tuple = cursor.fetchwarnings()  # <- returns a list of tuples

    cursor.execute("CREATE TABLE IF NOT EXISTS lastReadin (hpcID varchar(30) NOT NULL UNIQUE, lastReadinFile varchar(30) NOT NULL) ")
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
        # cursor.execute('CREATE INDEX index_nodelist ON ' + dbspec + '(nodelist)')
        cursor.execute('CREATE INDEX index_qos ON ' + dbspec + '(qos)')

        connection.commit()  # Commits any tables and indices that were created to the database
        print('Indexes created\n')
    elif (tuple[0][1] == 1050):
        print('Table already exists\n')

    date = '1999-01-01.txt'

    add_data = ("INSERT IGNORE INTO lastReadin (hpcID, lastReadinFile) "
                "VALUES (%(hpcID)s, %(lastReadinFile)s)")
    data = {
        'hpcID': tableName,
        'lastReadinFile': date,
    }
    cursor.execute(add_data, data, multi=True)
    connection.commit()
    cursor.close()

def timeConversion(raw):
    # Takes the "raw" minutes column and converting into a standard format based on raw data column format
    dash_position = raw.find('-')
    if (dash_position == 1):
        found = []
        if re.search('\:', raw) is not None:
            for i in re.finditer('\:', raw):
                found.append(i.start(0))
        if len(found) == 2:
            # (D-H:M:S) Format
            temp = re.split('[-]', raw)
            day = int(temp[0]) * 1440  # The amount of minutes in a day

            temp1 = temp[1]
            hms = re.split('[:]', temp1)

            h = int(hms[0]) * 60
            m = int(hms[1])
            s = int(hms[2]) * 0.0166667

            max_minutes = day + h + m + s

        if len(found) == 1:
            # (D-H:M) Format
            temp = re.split('[-]', raw)
            day = int(temp[0]) * 1440  # The amount of minutes in a day

            temp1 = temp[1]
            hms = re.split('[:]', temp1)
            h = int(hms[0]) * 60
            m = int(hms[1])

            max_minutes = day + h + m

        if re.search('\:', raw) is None:  # Working
            # Day-Hour (DH) Format
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
            # (H:M:S) Format
            hms = re.split('[:]', raw)
            h = int(hms[0]) * 60
            m = int(hms[1])
            s = int(hms[2]) * 0.0166667

            max_minutes = h + m + s

        if len(found) == 1:
            # (M:S) Format
            hms = re.split('[:]', raw)
            m = int(hms[0])
            s = int(hms[1]) * 0.0166667

            max_minutes = m + s

        if re.search('\:', raw) is None:  # Working
            # (MM) Format
            max_minutes = int(raw)

    return max_minutes

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

        cursor.execute("SELECT lastReadinFile FROM lastReadin WHERE hpcID='" + tableName + "'")
        hpcList = cursor.fetchall()
        tupleBreakdown = []
        tupleBreakdown += hpcList[0]
        lastReadinFile = tupleBreakdown[0]


        readIn = open(filename, 'r')
        firstline = next(readIn)

        # Establish this file's record size.
        record_size = len(firstline.split('|'))
        if record_size != SHORT_RECORD_LEN and record_size != QOS_RECORD_LEN:
            total_files_skipped += 1
            print("\nERROR: Records with", record_size, "fields are not supported, skipping file", filename)
            continue
        if lastReadinFile < filename:
        # Read the first line of the file to determine the record format.
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
                    print("\nERROR: Record has", size, "fields, expected", record_size, "fields in", filename, "line",
                          lineno)
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

                raw = str(row[7])  # Raw is the max_minutes column
                max_minutes = timeConversion(raw)

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
                cursor.execute("UPDATE lastReadin SET lastReadinFile = '" + filename + "' where hpcID = '" + tableName + "'") # Update the LRF of row of correct HPCID with correct LRF
            # Commit after writing all the data of the current file into the table
        else:
            continue
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

def intTryParse(value, filename, lineno):
    try:
        return int(value)
    except:
        global total_errors
        total_errors += 1
        print("\nERROR: Integer conversion in file ", filename, "line", lineno)
        return 0

def main():

    genConnection = connectGen() # Generally connect to MySQL Workbench
    databaseName = input('\nEnter the name of the MySQL database you would like to create your data tables in. If no name is inputted, the default database value will be used: ')
    createDatabase(genConnection, databaseName) # Checks to see if the inputted database name is equal to the default name or not, if not, create new database to access and create tables in

    connection = connect()
    tableName = input('Enter a name of a HPC machine you would like to store your job data into: ')
    createTable(connection, tableName)
    injection(connection, tableName)

    print("Total record errors: ", total_errors)
    print("Total files skipped: ", total_files_skipped)
    print("Total files read: ", filecount)

    connection.close()


if __name__ == '__main__':
    main()
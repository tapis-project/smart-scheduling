from __future__ import print_function
import sys
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode
import os
from os.path import exists
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
partition_limit = 2880 # Default time limit for max job runtimes in TACC HPC systems - 2880 Minutes or 2 Days
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
    '''
    The connect() function is a function that establishes a connection between the provided SQl user database and Python via the mysql.connector library package

    :return:
        connection: function variable that holds the connection properties to the provided SQL database to run certain commands in Python as if it were a SQL command
    '''
    connection = mysql.connector.connect(host=my_host, user=my_user, passwd=my_passwd, database=my_database)

    print('\nSuccessfully Connected to your MySQL Database:', my_database)

    return connection

def connectGen():
    '''
    The connectGen() function creates a general connection this python script and MySQL to run SQL commands. This is different than connect() as it is a more general connection,
    as in connect(), it is a connection that is tied to database and can only run SQL commands in that specific database. connectGen() can run SQL commands in a more general sense that
    is outside the scope of a specific database

    :return:
        genConnection: function variable that holds a general cursor-type attribute, which is passed into different functions that need to use a SQL command that can only be processed via
        the cursor
    '''

    genConnection = mysql.connector.connect(host=my_host, user=my_user, passwd=my_passwd)

    print('\nSuccessfully Connected to MySQL Server')

    return genConnection

def createDatabase(genConnection, databaseName):
    '''
    The createDatabase() function, provided with the genConnection and databaseName variables, creates a SQL database based on if it already exists. It will create one if it does not exist, and
    will use the provided default database if no database is requested

    :param genConnection: function provided variable that holds the SQL-Python connection property, which is used to create a temporary cursor object in this function
    :param databaseName: function provided object holding the user-defined SQL database name
    :return: None
        Returns None but creates a database if requested
    '''
    global my_database
    if databaseName == my_database: # User provided a database name that matches the default, as such the default database name from the global variables is used
        print('Default database ' + databaseName + ' in use...')

    else: # Else, set the my_database variable to the new user inputted one, create if not exists by general cursor

        my_database = databaseName
        genConnection.get_warnings = True
        cursor = genConnection.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS " + databaseName)
        tuple = cursor.fetchwarnings()

        if tuple is None:
            cursor.execute("SHOW DATABASES")
            for x in cursor:
                print(x)
        elif(tuple[0][1] == 1007): # Error code 1007 occurs if that database name already exists, as such error is handled
            print('\nDatabase ' + databaseName + ' already exists')

        cursor.close()

def createTable(connection, tableName):
    '''
    The createTable() function, provided with connection and tableName variable, creates a SQL table in the provided SQL database. This function will create a table of the requested HPC system
    if it does not already exist, as well as a lastReadin table, which is a table that stores the date of the last commited read in file to its respective HPC table to reduce overall runtime
    and increase efficiency.

    :param connection: function provided variable that holds a connection with the SQL instance, utilized by creating a temporary cursor object to run certain SQL commands
    :param tableName: function provided variable that holds the requested table name
    :return: None

    '''
    connection.get_warnings = True
    cursor = connection.cursor()

    # Established each column (name, type, length, null status) of the newly created table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS " + tableName + " (jobid varchar(30) NOT NULL PRIMARY KEY, user varchar(80) NOT NULL, account varchar(60) NOT NULL, "
                                                    "start datetime NOT NULL, end datetime NOT NULL, submit datetime NOT NULL, queue varchar(30) NOT NULL, "
                                                    "max_minutes int unsigned NOT NULL, jobname varchar(60) NOT NULL, state varchar(20) NOT NULL, nnodes int unsigned NOT NULL, "
                                                    "reqcpus int unsigned NOT NULL, nodelist TEXT NOT NULL, qos varchar(20))")
    tuple = cursor.fetchwarnings()  # <- returns a list of tuples

    cursor.execute("CREATE TABLE IF NOT EXISTS lastReadin (hpcID varchar(30) NOT NULL UNIQUE, lastReadinFile varchar(30) NOT NULL) ")
    if tuple is None:
        print('New table ' + tableName + ' generated')
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
    elif (tuple[0][1] == 1050): # Error code 1050 occurs if the table already exists, error handeling
        print('Table ' + tableName + ' already exists\n')

    date = '1999-01-01.txt' # Date must be a value that is before any of the dates the accounting was run on, as such a arbitary date long in the past was selected - Do not change

    add_data = ("INSERT IGNORE INTO lastReadin (hpcID, lastReadinFile) "
                "VALUES (%(hpcID)s, %(lastReadinFile)s)")
    data = {
        'hpcID': tableName,
        'lastReadinFile': date,
    }
    cursor.execute(add_data, data, multi=True) # Running command to create lastReadin table and inserting the 'date' variable into it
    connection.commit()
    cursor.close()

def timeConversion(raw):
    '''
    The timeConversion() function takes the unspliced Timelimit column from the provided accounting data and based on the data's different formating, converts the time into a standard format
    that is returned to the injection() function. In the SQL table, this column is renamed to max_minutes.

    :param raw: function provided object that holds the unspliced Timelimit data that holds the max amount of time a job can run for. This data is in a txt format and as such is convereted into a valid
    decimal time format, in this case, is converted into minutes. For instance, 2-00:00:00 would be converted into 2880 minutes. There are different formats, and are handeled below
    :return: max_minutes
        Object that holds the converted time value for the Timelimit column, which is returned to injection() function

    '''
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
            if raw == 'Partition_Limit':
               max_minutes = partition_limit
            else:
                max_minutes = int(raw)

    return max_minutes

def injection(connection, tableName):
    '''
    The injection() function finds the directory where the requested HPC accounting data is held, and line by line for each file, inserts the accounting data into its respective HPC SQL data table.
    injection() will not insert data if it previously has been inserted into the table, utilized by the functionality of the lastReadin SQL data table, which checks to see the current filename
    is newer than the date of the last commited file

    Each accounting data for each submitted job is in the following format the majority of time except for certain cases:
    JobID|User|Account|Start|End|Submit|Partition|Timelimit|JobName|State|NNodes|ReqCPUS|NodeList|QOS
    As such, each line is spliced and converted into proper data types

    :param connection: function provided variable that holds a connection with the SQL instance, utilized by creating a temporary cursor object to run certain SQL commands
    :param tableName: function provided variable that holds the requested table name
    :return: None
        None is returned but the accounting data is inserted into the SQL data table
    '''
    # Assign the actual directory that contains all the input files.

    dashCheck = my_parent_dir[-1]
    if dashCheck != '/':
        adjustment = my_parent_dir + '/' #Adds / to provided accounting data directory path if user didn't specify correctly path
        source = adjustment + tableName
    else:
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
        # tupleBreakdown will only access the first value in hpcList because hpcList is a tuple that carries only the last readin filename (which in our specific case is the last date the
        # data was commited to the HPC data table -> EX. HPCList:  [('2022-10-05.txt',)]
        # As such, the tuple must be spliced into a text format which is used in the comparison with the filename to see if the currently reviewed filename is newer than the last readin file
        tupleBreakdown += hpcList[0]
        lastReadinFile = tupleBreakdown[0]


        if lastReadinFile < filename: # If the current filename is newer than the last readin file, insert accounting data to table

            fileSize = os.path.getsize(filename)
            fileExists = exists(source + '/' + filename)
            if fileSize == 0 or fileExists == False:
                total_files_skipped += 1
                print("\nERROR: File: ", filename, " is empty, skipping file") # Error handeling - empty or non existant files
                continue

            else:
                readIn = open(filename, 'r')
                firstline = next(readIn)
                # Establish this file's record size.
                record_size = len(firstline.split('|'))
                if record_size != SHORT_RECORD_LEN and record_size != QOS_RECORD_LEN:
                    total_files_skipped += 1
                    print("\nERROR: Records with", record_size, "fields are not supported, skipping file", filename) # Error handeling if the feilds in the data are not curretnly supported by table
                    continue

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

                    # Conversion to UTC for start, end and submit column variables
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
                # Commit after writing all the data of the current file into the table
                connection.commit()
                cursor.execute("UPDATE lastReadin SET lastReadinFile = '" + filename + "' where hpcID = '" + tableName + "'") # Update the LRF of row of correct HPCID with correct LRF


        elif lastReadinFile >= filename: # This means for loop has gotten to most recent file that has been inserted
            # As such, skip insert
            continue

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
    '''
    The intTryParse() function documents any attempted files that have an issue (Different amount of variables in the data, etc.)
    :param value:
    :param filename: function provided object that holds the currently read filename in the injection() function
    :param lineno: function provided value that holds the current line that is being read in for each respective file name
    :return: None
    '''
    try:
        return int(value)
    except:
        global total_errors
        total_errors += 1
        print("\nERROR: Integer conversion in file ", filename, "line", lineno)
        return 0

def main():

    genConnection = connectGen()

    while len(sys.argv) != 3:
        try:

            print("Please enter the correct amount of command-line arguments (2) in their respective order: "
                      "\npython3 HPCDataLoad.py [Database Name] [Table Name]")
            sys.exit(1)
        except ValueError:
                print("Incorrect number of arguments submitted, please make sure to enter the correct amount of command-line arguments (2) in their respective order: "
                      "\npython3 HPCDataLoad.py [Database Name] [Table Name]")

    databaseName = sys.argv[1]
    tableName = sys.argv[2]
    createDatabase(genConnection,
                   databaseName)  # Checks to see if the inputted database name is equal to the default name or not, if not, create new database to access and create tables in

    connection = connect()
    createTable(connection, tableName)
    injection(connection, tableName)

    print("Total record errors: ", total_errors)
    print("Total files skipped: ", total_files_skipped)
    print("Total files read: ", filecount)

    connection.close()
    sys.exit(1)
if __name__ == '__main__':
    main()
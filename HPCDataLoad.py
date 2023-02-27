from __future__ import print_function
import sys
from datetime import datetime
import mysql.connector
import os
from os.path import exists
import re
import pytz
import linecache

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
my_user = "costaki"  # Connection instance username that has the ability to create and modify tables, indexes and databases
my_passwd = "am_)jRsFjPo9ZL0"  # Password for the user with the access mentioned on the line above
my_database = "HPC_Job_Database"  # The MySQL variable that hosts the name of the database that the tables of the submitted data will be stored on (Variable name to change at discretion of user)
my_parent_dir = "/home/ubuntu/jobs_data/"  # The parent directory of the HPC-specific input directories that host the submitted job data that will be inserted into the MySQL table
partition_limit = 2880  # Default time limit for max job runtimes in TACC HPC systems - 2880 Minutes or 2 Days
# **************************************************************

# ---------------------------------------------------
# GLOBAL variables and constants.
total_errors = 0
total_files_skipped = 0
total_field_errors = 0
filecount = 0

# Number of pipe-separated fields in raw input files
SHORT_RECORD_LEN = 13
QOS_RECORD_LEN = 14

# Data field naming for easier readability when calling specific elements in an array
JOBID = 0
USER = 1
ACCOUNT = 2
START_TIME = 3
END_TIME = 4
SUBMIT_TIME = 5
QUEUE_TYPE = 6
MAX_MINUTES = 7
JOBNAME = 8
JOB_STATE = 9
NNODES = 10
REQCPUS = 11
NODELIST = 12
QOS = 13


# ---------------------------------------------------

def connect():
    '''
    The connect() function is a function that establishes a connection between the provided SQl user database and Python via the mysql.connector library package.

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
    is outside the scope of a specific database.

    :return:
        genConnection: Function variable that holds a general cursor-type attribute, which is passed into different functions that need to use a SQL command that can only be processed via
        the cursor.
    '''

    genConnection = mysql.connector.connect(host=my_host, user=my_user, passwd=my_passwd)

    print('\nSuccessfully Connected to MySQL Server')

    return genConnection


def createDatabase(genConnection, databaseName):
    '''
    The createDatabase() function, provided with the genConnection and databaseName variables, creates a SQL database based on if it already exists. It will create one if it does not exist, and
    will use the provided default database if no database is requested.

    :param genConnection: Function provided variable that holds the SQL-Python connection property, which is used to create a temporary cursor object in this function.
    :param databaseName: Function provided object holding the user-defined SQL database name.
    :return: None
        Returns None but creates a database if requested.

    '''
    global my_database
    if databaseName == my_database:  # User provided a database name that matches the default, as such the default database name from the global variables is used
        print('Default database ' + databaseName + ' in use...')

    else:  # Else, set the my_database variable to the new user inputted one, create if not exists by general cursor

        my_database = databaseName
        genConnection.get_warnings = True
        cursor = genConnection.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS " + databaseName)
        tuple = cursor.fetchwarnings()

        if tuple is None:
            cursor.execute("SHOW DATABASES")
            for x in cursor:
                print(x)
        elif (tuple[0][1] == 1007):  # Error code 1007 occurs if that database name already exists, as such error is handled
            print('\nDatabase ' + databaseName + ' already exists')

        cursor.close()


def createTable(connection, tableName):
    '''
    The createTable() function, provided with connection and tableName variable, creates a SQL table in the provided SQL database. This function will create a table of the requested HPC system
    if it does not already exist, as well as a lastReadin table, which is a table that stores the date of the last commited read in file to its respective HPC table to reduce overall runtime
    and increase efficiency.

    :param connection: Function provided variable that holds a connection with the SQL instance, utilized by creating a temporary cursor object to run certain SQL commands.
    :param tableName: Function provided variable that holds the requested table name.
    :return: None

    '''
    connection.get_warnings = True
    cursor = connection.cursor()

    # Established each column (name, type, length, null status) of the newly created table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS " + tableName + " (jobid varchar(30) NOT NULL PRIMARY KEY, user varchar(80) NOT NULL, account varchar(64) NOT NULL, "
                                                    "start datetime NOT NULL, end datetime NOT NULL, submit datetime NOT NULL, queue varchar(30) NOT NULL, "
                                                    "max_minutes int unsigned NOT NULL, jobname varchar(64) NOT NULL, state varchar(20) NOT NULL, nnodes int unsigned NOT NULL, "
                                                    "reqcpus int unsigned NOT NULL, nodelist TEXT NOT NULL, qos varchar(20))")
    tuple = cursor.fetchwarnings()  # <- returns a list of tuples

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS lastReadin (hpcID varchar(30) NOT NULL UNIQUE, lastReadinFile varchar(30) NOT NULL) ")
    if tuple is None:
        print('New table ' + tableName + ' generated')
        # ALl permissions granted, no warning message or message in general outputted to the user, no need for conditional statements
        dbspec = my_database + '.' + tableName

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
    elif (tuple[0][1] == 1050):  # Error code 1050 occurs if the table already exists, error handeling
        print('Table ' + tableName + ' already exists\n')

    date = '1999-01-01.txt'  # Date must be a value that is before any of the dates the accounting was run on, as such a arbitary date long in the past was selected - Do not change

    add_data = ("INSERT IGNORE INTO lastReadin (hpcID, lastReadinFile) "
                "VALUES (%(hpcID)s, %(lastReadinFile)s)")
    data = {
        'hpcID': tableName,
        'lastReadinFile': date,
    }
    cursor.execute(add_data, data, multi=True)  # Running command to create lastReadin table and inserting the 'date' variable into it
    connection.commit()
    cursor.close()


def timeConversion(raw):
    '''
    The timeConversion() function takes the unspliced Timelimit column from the provided accounting data and based on the data's different formating, converts the time into a standard format
    that is returned to the injection() function. In the SQL table, this column is renamed to max_minutes.

    :param raw: Function provided object that holds the unspliced Timelimit data that holds the max amount of time a job can run for. This data is in a txt format and as such is convereted into a valid
    decimal time format, in this case, is converted into minutes. For instance, 2-00:00:00 would be converted into 2880 minutes. There are different formats, and are handeled below.
    :return: max_minutes
        Object that holds the converted time value for the Timelimit column, which is returned to injection() function.

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
            case1 = 'Partition_Limit'  # Check to see if raw is not in the typical time-format and as such passing in global variable partition_limit
            case2 = 'Partition Limit'  # if matches test cases
            case3 = ""
            if raw.casefold() == case1.casefold() or raw.casefold() == case2.casefold() or raw.casefold() == case3.casefold():
                max_minutes = partition_limit
            else:
                max_minutes = int(raw)
    elif (dash_position == 2):
        found = []
        if re.search('\:', raw) is not None:
            for i in re.finditer('\:', raw):
                found.append(i.start(0))
        if len(found) == 2:
            # (DD-H:M:S) Format
            temp = re.split('[-]', raw)
            day = (int(temp[0]) * 1440)  # The amount of minutes in a day

            temp1 = temp[1]
            hms = re.split('[:]', temp1)

            h = int(hms[0]) * 60
            m = int(hms[1])
            s = int(hms[2]) * 0.0166667

            max_minutes = day + h + m + s

    return max_minutes


def injection(connection, tableName):
    '''
    The injection() function finds the directory where the requested HPC accounting data is held, and line by line for each file, inserts the accounting data into its respective HPC SQL data table.
    injection() will not insert data if it previously has been inserted into the table, utilized by the functionality of the lastReadin SQL data table, which checks to see the current filename
    is newer than the date of the last committed file.

    Each accounting data for each submitted job is in the following format the majority of time except for certain cases:
    JobID|User|Account|Start|End|Submit|Partition|Timelimit|JobName|State|NNodes|ReqCPUS|NodeList|QOS
    As such, each line is spliced and converted into proper data types.

    :param connection: Function provided variable that holds a connection with the SQL instance, utilized by creating a temporary cursor object to run certain SQL commands.
    :param tableName: Function provided variable that holds the requested table name.
    :return: None
        None is returned but the accounting data is inserted into the SQL data table.

    '''
    # Assign the actual directory that contains all the input files.

    dashCheck = my_parent_dir[-1]
    if dashCheck != "/":
        adjustment = my_parent_dir + "/"  # Adds / to provided accounting data directory path if user didn't specify correctly path
        source = adjustment + tableName
    else:
        source = my_parent_dir + tableName

    os.chdir(source)

    local = pytz.timezone("US/Central")
    global filecount
    global total_errors
    global total_files_skipped
    global total_field_errors

    start = datetime.now()
    print('Start: ', start)
    for filename in sorted(os.listdir(source)):
        # Skip directories.
        if not os.path.isfile(filename):
            continue

        if filename == "errorlog.txt":  # Skips reading in text file that holds all errors while inserting the accounting data
            continue

        # Open a new cursor in existing connection.
        cursor = connection.cursor()

        cursor.execute("SELECT lastReadinFile FROM lastReadin WHERE hpcID='" + tableName + "'")
        hpcList = cursor.fetchall()
        tupleBreakdown = []  # tupleBreakdown will only access the first value in hpcList because hpcList is a tuple that carries only the last readin filename (which in our specific case is the last date the
        tupleBreakdown += hpcList[0]  # data was commited to the HPC data table -> EX. HPCList:  [('2022-10-05.txt',)]
        lastReadinFile = tupleBreakdown[0]  # As such, the tuple must be spliced into a text format which is used in the comparison with the filename to see if the currently reviewed filename is newer than the last readin file
        if lastReadinFile >= filename:  # This means for loop has gotten to most recent file that has been inserted
            # As such, skip insert
            continue
        # If the current filename is newer than the last readin file, insert accounting data to table

        fileSize = os.path.getsize(filename)
        fileExists = exists(source + '/' + filename)
        permissionCode = oct(os.stat(filename).st_mode & 0o777)[2:]  # Returns the permission code (P.C) of filename (P.C being what degree of read access is available for a specific file)
        readAccess = os.access(filename, os.R_OK)  # Returns bool value of whether a file has read access

        if fileSize == 0 or fileExists is False:
            total_files_skipped += 1
            print("\nERROR: File ", filename, " is empty, skipping file")  # Error handling - empty/non existant files
            writeError(errorStatement=f"\nERROR: File {filename} is empty, skipping file\n")
            continue

        elif readAccess is False:
            total_files_skipped += 1
            print("\nERROR: File: ", filename, " is inaccessible, cannot be read due to chmod permission code ",
                  permissionCode, ", skipping file")  # Error handling - lacking read permission access to file
            writeError(
                errorStatement=f"\nERROR: File {filename} is inaccessible, cannot be read due to chmod permission code {permissionCode}, skipping file\n")
            continue

        badFirstLnResult = detectBadFirstln(filename)
        if badFirstLnResult is True:  # Bad first and second line, skip entire file
            continue

        readIn = open(filename, "r")
        firstline = next(readIn)
        # Establish this file's record size.
        record_size = len(firstline.split("|"))
        if type(badFirstLnResult) == int:  # If the first line is bad but the second line is good detectBadFirstln() sets the record size to the length of the good record size and returns said value
            record_size = badFirstLnResult

        # Read the first line of the file to determine the record format.
        # Read the rest of the file line by line.
        lineno = 1
        try:
            for line in readIn:
                # Assign line number
                lineno += 1

                # Parse next line and validate number of fields
                row = line.split('|')
                size = len(row)
                if size != record_size:
                    total_errors += 1
                    print("\nERROR: Record has", size, "fields, expected", record_size, "fields in", filename, "line " + str(lineno) + ", attempting repair")
                    writeError(errorStatement=f"\nERROR: Record has {size} fields, expected {record_size} fields in file {filename} line {lineno}\n{line}Attempting repair...\n")
                    if row[0] == "|":
                        print("ERROR: Line ", lineno, "in file " + filename + ", started with a pipe, skipping line")
                        writeError(errorStatement=f"ERROR: Line {lineno} in file {filename}, started with a pipe, skipping line\n")
                        continue
                    if row[size - 1] == "" or row[size-1] == "\n":
                        ele = row.pop() # Removes last index if it is empty due to an unnecessary pipe
                    repairOutcome = lineRepair(row, record_size, filename, lineno, line)  # Due to there being more than the expected amount of fields in the line, send to be repaired
                    if repairOutcome is False:
                        # If lineRepair is unable to repair, print error, also print in general to notify the user
                        print("ERROR: Unsuccessful repair of line", lineno, "in file " + filename + ", skipping line")
                        writeError(errorStatement=f"ERROR: Unsuccessful repair of line {lineno} in file {filename}\n")
                        continue
                    # else, use the return values and insert into dataset
                    elif repairOutcome is not False:
                        print("SYSTEM: Successful repair of line", lineno, "in file ", filename, "\n")
                        writeError(errorStatement=f"SYSTEM: Successful repair of line {lineno} in file {filename}\n")
                        repairLineInsertAttempt = insertRepair(repairOutcome, cursor, tableName)
                        if repairLineInsertAttempt is True:
                            continue


                # Assign fields from left to right.
                jobid = str(row[JOBID])
                user = str(row[USER])
                account = str(row[ACCOUNT])

                # Conversion to UTC for start, end and submit column variables
                local_start = datetime.strptime(row[START_TIME], '%Y-%m-%dT%H:%M:%S')
                local_dt_strt = local.localize(local_start, is_dst=True)
                start = local_dt_strt.astimezone(pytz.utc)

                local_end = datetime.strptime(row[END_TIME], '%Y-%m-%dT%H:%M:%S')
                local_dt_end = local.localize(local_end, is_dst=True)
                end = local_dt_end.astimezone(pytz.utc)

                local_submit = datetime.strptime(row[SUBMIT_TIME], '%Y-%m-%dT%H:%M:%S')
                local_dt_submit = local.localize(local_submit, is_dst=True)
                submit = local_dt_submit.astimezone(pytz.utc)

                # If statements to handle bad job submit, start, and end times 
                # Meaning that a job's submit, start, and end times potentially don't logically make sense, such as a job ending before it even starting
                
                if submit > start: # If the job submit time is > job start time, job is now considered bad data, as such the job is not being written to the database
                    print("ERROR: Record on line", lineno, "in file", filename, "has a submit time greater than start time. Job data is now identified as faulty, skipping line")
                    writeError(errorStatement=f"\nERROR: Record on line {lineno} in file {filename} has a submit time greater than start time. Job is now identified as faulty, skipping line")
                    continue # Skip line 
                    
                if start > end: # Skip line if the job's start time is greater than it's end time 
                    print("ERROR: Record on line", lineno, "in file", filename, "has a start time greater than it's end time. Job data is now identified as faulty, skipping line")
                    writeError(errorStatement=f"\nERROR: Record on line {lineno} in file {filename} has a start time greater it's than end time. Job data is now identified as faulty, skipping line")
                    continue  # Skip line 
                    
                if submit > end: # Skip line if the job's submit time is greater than it's end time
                    print("ERROR: Record on line", lineno, "in file", filename, "has a submit time greater than it's end time. Job data is now identified as faulty, skipping line")
                    writeError(errorStatement=f"\nERROR: Record on line {lineno} in file {filename} has a submit time greater it's than end time. Job data is now identified as faulty, skipping line")
                    continue  # Skip line 

                queue = str(row[QUEUE_TYPE])

                raw = str(row[MAX_MINUTES])

                max_minutes = timeConversion(raw)

                jobname = str(row[JOBNAME])

                state = str(row[JOB_STATE])

                nnodes = intTryParse(row[NNODES], filename, lineno, line)

                reqcpus = intTryParse(row[REQCPUS], filename, lineno, line)

                nodelisttmp = str(row[NODELIST])
                nodelist = nodelisttmp.strip("\n")  # Removes \n at the end of nodelist
                # Optional fields depending on record length.
                qos = None
                if record_size == QOS_RECORD_LEN:
                    qos = str(row[QOS])

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
            cursor.execute("UPDATE lastReadin SET lastReadinFile = '" + filename + "' where hpcID = '" + tableName + "'")  # Update the LRF of row of correct HPCID with correct LRF
            connection.commit()
        except ValueError:
            print("\nVALUE ERROR: Program exited due to a error of one the datapoints on line", lineno, "in file", filename, "\n", line, "\nSkipping line")
            writeError(errorStatement=f"VALUE ERROR: Program threw out line due to a error of one the datapoints on line {lineno} in file {filename}\n{line}\nSkipping line")
            continue
            #sys.exit(1)

        # Print progress message over the last message without newline.
        filecount += 1
        progress = "\nLast file committed: " + str(filecount) + " - " + filename + "       "
        print(progress, end='\r')

        # Clean up.
        cursor.close()
        readIn.close()

    end = datetime.now()
    print('\nEnd: ', end)
    runTime = end - start
    print("\nScript run time: ", runTime, "\n")
    writeError(errorStatement=f"Script ran for {runTime}")

def detectBadFirstln(filename):
    '''
    The detectBadFirstln() function checks to see if the first line of an accounting dataset text file is the expected header that details the columns names. If this is not the case
    and rather the text file goes straight into the dataset, detectBadFirstln() will check if the first lines record size is equal to the expected value of either SHORT_RECORD_LEN or
    QOS_RECORD_LEN. If this not the case and instead the first line is faulty (defined as where the record size is < SHORT_RECORD_LEN), detectBadFirstln() checks to see if the following line is
    equal to the expected value. If this is the case, detectBadFirstln() sets the record_size to the value of the record size of the second line. If both the first and second line are still
    < SHORT_RECORD_LEN, the entire file is treated as faulty and is skipped. detectBadFirstln() returns either a boolean (True if the file is faulty or False if the file is good) or record_size based on the conditions
    mentioned previously.

    :param filename: Function provided object the holds the current file being read in by the injection() function.
    :return: True/False (True if the file is faulty or False if the file is good) or record_size (If the first line is faulty but the second line(And presumingly the rest of the file) is not faulty).

    '''
    global SHORT_RECORD_LEN
    global QOS_RECORD_LEN
    global total_errors
    global total_field_errors
    global total_files_skipped

    readIn = open(filename, "r")
    firstline = next(readIn)
    record_size = len(firstline.split("|"))

    if record_size not in [SHORT_RECORD_LEN, QOS_RECORD_LEN]:  # Case where the first line is not the expected formatting line and goes straight into the dataset instead
        if record_size < SHORT_RECORD_LEN:
            total_errors += 1
            total_field_errors += 1
            firstlineno = 1
            writeError(errorStatement=f"\nERROR: Line {firstlineno} in file {filename} has {record_size} fields, which is not supported due to missing information, skipping line\n{firstline}\n")
            secondline = linecache.getline(filename, 2)
            record_size2 = len(secondline.split("|"))
            if record_size2 not in [SHORT_RECORD_LEN, QOS_RECORD_LEN]:
                total_files_skipped += 1
                print("\nERROR: Records with", record_size2, "fields are not supported, skipping file", filename)  # Error handling if the fields in the data are not currently supported by table
                writeError(errorStatement=f"\nERROR: Records with {record_size2} fields are not supported, skipping file {filename}\n")
                return True
            else:
                record_size = record_size2
                return record_size
        elif record_size > QOS_RECORD_LEN:
            # For instance where the firstline is not the expected formatting line and is instead accounting data that needs corrections due to extra unnecessary columns, send to be reparsed
            lineRepair(row = firstline, record_size = record_size, filename = filename, lineno = 1, line = firstline)

    return False


def lineRepair(row, record_size, filename, lineno, line):
    '''
    The lineRepair() function takes the current row that is being attempted to be inserted to the data table, but is unsuccesful to do so due to extra columns in the row, and corrects the line.
    This is done, in this current iteration of this function tackles the case where the extra columns that exist are only between the jobname and state columns, by taking the current number of columns in the row and compares
    that value to the expected number of columns the current file that is being read in specifies. The difference between these two values is taken to account, and as such the function iterates x amount of times until
    the difference is met, combining the extra column(s) data into one unified object, and adds it to the jobname column (with "?" marks included to note that these new additions were not how the data was originally).
    An error is written to the errorlog.txt file documenting this effort to notify the user to correct their dataset, and inserts this corrected row to the dataset.
    :return:

    '''
    global QOS_RECORD_LEN
    global SHORT_RECORD_LEN
    local = pytz.timezone("US/Central")
    size = len(row)  # Must match record_size (Record Size of the file)

    if size < record_size:
        return False

    jobid = str(row[JOBID])
    user = str(row[USER])
    account = str(row[ACCOUNT])

    local_start = datetime.strptime(row[START_TIME], '%Y-%m-%dT%H:%M:%S')
    local_dt_strt = local.localize(local_start, is_dst=True)
    start = local_dt_strt.astimezone(pytz.utc)

    local_end = datetime.strptime(row[END_TIME], '%Y-%m-%dT%H:%M:%S')
    local_dt_end = local.localize(local_end, is_dst=True)
    end = local_dt_end.astimezone(pytz.utc)

    local_submit = datetime.strptime(row[SUBMIT_TIME], '%Y-%m-%dT%H:%M:%S')
    local_dt_submit = local.localize(local_submit, is_dst=True)
    submit = local_dt_submit.astimezone(pytz.utc)

    queue = str(row[QUEUE_TYPE])

    raw = str(row[MAX_MINUTES])
    max_minutes = timeConversion(raw)

    jobname = str(row[JOBNAME])
    compilation = ""
    qos = None

    if record_size == QOS_RECORD_LEN:
        print("Record Size", record_size)
        deltaSteps = size - record_size  # The number of extra fields

        qos = str(row[size - 1])  # row[QOS]
        nodelisttmp = str(row[size - 2])  # row[NODELIST]
        nodelist = nodelisttmp.strip("\n")  # Removes \n at the end of nodelist
        reqcpus = intTryParse(row[size - 3], filename, lineno, line)
        nnodes = intTryParse(row[size - 4], filename, lineno, line)
        state = str(row[size - 5])

        compilationtmp = ""

        for i in range(1, deltaSteps + 1):
            compilationtmp += ("?" + str(row[size - 4 - i]))
            compilation = compilationtmp.replace(" ", "")
        jobname += compilation
        return jobid, user, account, start, end, submit, queue, max_minutes, jobname, state, nnodes, reqcpus, nodelist, qos

    elif record_size == SHORT_RECORD_LEN:
        deltaSteps = size - record_size  # The number of extra fields
        nodelisttmp = str(row[size - 1])  # row[NODELIST]
        nodelist = nodelisttmp.strip("\n")  # Removes \n at the end of nodelist
        reqcpus = intTryParse(row[size - 2], filename, lineno, line)
        nnodes = intTryParse(row[size - 3], filename, lineno, line)
        state = str(row[size - 4])

        compilationtmp = ""

        for i in range(1, deltaSteps + 1):
            compilationtmp += ("?" + str(row[size - 4 - i]))
            compilation = compilationtmp.replace(" ", "")
        jobname += compilation
        return jobid, user, account, start, end, submit, queue, max_minutes, jobname, state, nnodes, reqcpus, nodelist, qos

def insertRepair(repairResult, cursor, tableName):
    '''
    The repairResult() function takes the repairResult tuple object, breaks it up into its representative objects necessary to be inserted into
    the SQL database, and inserts it into the database.
    :param repairResult: Tuple object that holds the return result from the lineRepair() function
    :param cursor: Object that holds the cursor capability to run commands in MySQL Workbench with the mysql-python connector library package
    :param connection: Object that holds the basic connection between this script and the MySQL HPC Job Database
    :return: bool, returns True to indicate the insertion was successfully made
    '''

    jobid, user, account, start, end, submit, queue, max_minutes, jobname, state, nnodes, reqcpus, nodelist, qos = repairResult
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
    return True


def intTryParse(value, filename, lineno, line):
    '''
    The intTryParse() function documents any attempted files that have an issue (Different amount of variables in the data, etc.).
    The intTryParse() function documents any attempted files that have an issue (Different amount of variables in the data, etc.).
    :param value: Function provided object that holds the parsed value of either nnodes or reqcpus.
    :param filename: Function provided object that holds the currently read filename in the injection() function.
    :param lineno: Function provided value that holds the current line number that is being read in for each respective file name.
    :param line: Function provided value that holds the current line that is being read in so the user can look at exactly where the error is.
    :return: None

    '''
    try:
        return int(value)
    except:
        global total_errors
        total_errors += 1
        print("\nERROR: Integer conversion in file ", filename, "line", lineno, "Repairing...")

        if value.find("K") != -1:
            repairtmp = "".join([a for a in value if a.isdigit()])
            repair = repairtmp * 1000  # Many of the instances have been a value of K, representing 1000, as such multiplying
            print("\nNnode value was successfully repaired")
            writeError(errorStatement=f"\nERROR: Integer conversion in file {filename} on line {lineno}\n{line}Value was repaired sucessfully\n")
            return repair
        else:
            writeError(errorStatement=f"\nERROR: Integer conversion in file {filename} on line {lineno}\n{line}Value was unsucessfully repaired\n")
            return 0


def writeError(errorStatement):
    '''
    The writeError() function writes all non-fatal errors that were outputted while inserting the accouting data to "errorlog.txt", a file stored on the directory of the HPC being readin.
    :param errorStatement: Function provided string object that holds the error statement that was printed.
    :return: None
    Returns none but creates and appends "errorlog.txt" with each error statement printed due to any addressed issue.

    '''
    with open("errorlog.txt", "a") as f:
        f.write(errorStatement)


def main():
    global my_database

    genConnection = connectGen()

    while len(sys.argv) != 2:
        try:

            print("Please enter the correct amount of command-line arguments (2) in their respective order: "
                  "\npython3 HPCDataLoad.py [Table Name]")
            sys.exit(1)
        except ValueError:
            print(
                "Incorrect number of arguments submitted, please make sure to enter the correct amount of command-line arguments (2) in their respective order: "
                "\npython3 HPCDataLoad.py [Table Name]")

    databaseName = my_database
    tableName = sys.argv[1]
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

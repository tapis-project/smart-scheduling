from __future__ import print_function
import logging
from typing import List
import socket
import math
import shutil
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode
import os

def isConnected(connection):
    print('Successfully Connected to SQL Database')

def createIfNotExists(connection, cursor, tableName):

    '''

    :param connection: Connection parameter to the SQL database
    :param cursor: Cursor element that allows access to different tools in the SQL database
    :param tableName: User provided tableName
    :return: Returns print statement based off condition whether a table was created if it did not exist, as well as indices that would be created if the table did not exist

    '''

    connection.get_warnings = True
    cursor.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (jobid int unsigned NOT NULL PRIMARY KEY, user varchar(80) NOT NULL, account varchar(50) NOT NULL, start datetime NOT NULL, end datetime NOT NULL, submit datetime NOT NULL, queue TEXT NOT NULL, timelimit TEXT NOT NULL, jobname TEXT NOT NULL, state TEXT NOT NULL, nnodes int unsigned NOT NULL, reqcpus int unsigned NOT NULL, nodelist TEXT NOT NULL)")
    tuple = cursor.fetchwarnings() # <- returns a list of tuples
    if tuple is None:
        print('New table generated\nCreating indexes')
        # ALl permissions granted, no warning message or message in general outputted to the user, no need for conditional statements
        cursor.execute('CREATE INDEX datatimesort ON HPC_Job_Time_Data.' + tableName + '(submit, start, end)')
        cursor.execute('CREATE INDEX nodesort ON HPC_Job_Time_Data.' + tableName + '(nnodes, reqcpus)')
        connection.commit() # Commits any tables and indices that were created to the database

    elif(tuple[0][1] == 1050):
        print('Table already exists, error code was raised successfully')

def injection(connection, cursor, tableName):
#Injects data into a specified table
    source = '/home/ubuntu/jobs_data/' + tableName + '/'
    archive = source + 'Archive/'
    os.chdir(source)

    for filename in os.listdir(source):
        readIn = open('test_file.txt', 'r')
        next(readIn)
        converted = []
        for line in readIn:
            row = line.split('|')
            jobid = int(row[0])
            user = str(row[1])
            account = str(row[2])
            start = datetime.strptime(row[3], '%Y-%m-%dT%H:%M:%S')
            end = datetime.strptime(row[4], '%Y-%m-%dT%H:%M:%S')
            submit = datetime.strptime(row[5], '%Y-%m-%dT%H:%M:%S')
            queue = str(row[6])
            timelimit = str(row[7]) # <- Need to convert properly to datetime object
            jobname = str(row[8])
            state = str(row[9]) # <- Need to fix? It's a state, COMPLETED OR FAILED
            nnodes = int(row[10])
            reqcpus = int(row[11])
            nodelist = str(row[12])

            # %s = treated and presented as a string
            # %d = treated as an integer and presented as a signed decimal number

            add_data = ("INSERT IGNORE INTO " + tableName +
                        "(jobid, user, account, start, end, submit, queue, timelimit, jobname, state, nnodes, reqcpus, nodelist) "
                        "VALUES (%(jobid)s, %(user)s, %(account)s, %(start)s, %(end)s, %(submit)s, %(queue)s, %(timelimit)s, %(jobname)s, %(state)s, %(nnodes)s, %(reqcpus)s, %(nodelist)s)")

            data = {
                'jobid': jobid,
                'user': user,
                'account': account,
                'start': start,
                'end': end,
                'submit': submit,
                'queue': queue,
                'timelimit': timelimit,
                'jobname': jobname,
                'state': state,
                'nnodes': nnodes,
                'jobname': jobname,
                'state': state,
                'nnodes': nnodes,
                'reqcpus': reqcpus,
                'nodelist': nodelist,
            }

            #print('Add data statement: ', add_data)

            cursor.execute(add_data, data)
            connection.commit()
            #print('Iterate through again') # Need to add statement based on the warning code of whether it was successful or not



def main():
    tableName = input('Enter a name of a HPC machine you would like to access: ')
    connection = mysql.connector.connect(host="127.0.0.1", user='costaki', passwd='password', database="HPC_Job_Time_Data")
    isConnected(connection) # Good
    cursor = connection.cursor()

    createIfNotExists(connection, cursor, tableName)
    injection(connection, cursor, tableName)

    connection.close()
    cursor.close()

if __name__ == '__main__':
    main()
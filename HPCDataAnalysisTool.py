from __future__ import print_function
import sys
from datetime import datetime
import mysql.connector
import pandas as pd
from mysql.connector import errorcode
import os
from os.path import exists
import re
import pytz

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
partition_limit = 2880  # Default time limit for max job runtimes in TACC HPC systems - 2880 Minutes or 2 Days
# **************************************************************


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

def main():

    connection = connect()

if __name__ == '__main__':
    main()
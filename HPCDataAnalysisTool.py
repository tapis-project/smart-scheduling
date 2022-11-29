from __future__ import print_function
import sys
import pandas as pd
import sqlalchemy as sa
import mysql.connector
import matplotlib.pyplot as plt
import numpy as np

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
# **************************************************************


def connect():
    '''
    The connect() function is a function that establishes a connection between the provided SQl user database and Python via the SQLAlchemy library package

    :return:
        connection: function variable that holds the connection properties to the provided SQL database to run certain commands in Python as if it were a SQL command
    '''
    try:
        connection = sa.create_engine("mysql+pymysql://" + my_user + ":" + my_passwd + "@" + my_host + "/" + my_database)
        print('\nSuccessfully Connected to your MySQL Database:', my_database)

        return connection
    except Exception as e:
        connection.close()
        print(str(e))

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

def query(connection, tableName, queueType, numNodes, state, max_minutes):
    '''
    The query() function creates a Pandas dataframe utilizing the user provided parameters to create statistical graphs. The graphs can be used to develop inferences in what is occuring in the dataset.
    :param connection: Object that holds the established SQL-Python connection to the database
    :param tableName: Object holding the table name the user is requesting. Can either be specified to one specific table or all tables in the database
    :param queueType: Object holding the requested queue type to be analyzed
    :param numNodes: Object holding the requested number of HPC nodes to be analyzed
    :param state: Object holding the requested job state to be analyzed
    :param max_minutes: Object holding the requested maximum amount of minutes a job can run for, utilized for specified analysis
    :return:
        Returns a specified Pandas dataframe, as well as corresponding graphs.
    '''
    allcase = "All"
    if tableName.casefold() == allcase.casefold():
        query = ("SELECT tablename.jobid, tablename.user, tablename.account, TIMESTAMPDIFF(minute, tablename.submit, tablename.start) AS queueTime, TIMESTAMPDIFF(minute, tablename.start, tablename.end) AS runTime, "
                 "tablename.queue, tablename.max_minutes, tablename.state, tablename.nnodes, tablename.reqcpus, tablename.nodelist"
                    " FROM frontera, lonestar6, stampede, stampede2, maverick WHERE queue LIKE '" + queueType + "' AND nnodes LIKE '" + numNodes + "' AND state LIKE '" + state + "'")
    else:
        query = ("SELECT jobid, user, account, TIMESTAMPDIFF(minute, submit, start) AS queueTime, TIMESTAMPDIFF(minute, start, end) AS runTime, queue, max_minutes, state, nnodes, reqcpus, nodelist"
            " FROM " + tableName +  " WHERE queue LIKE '" + queueType + "' AND nnodes LIKE '" + numNodes + "' AND state LIKE '" + state + "' AND max_minutes BETWEEN 1 AND " + max_minutes + "")

    df = pd.read_sql(query, connection)
    print(df)

def rangeQuery(connection, tableName, queueType, numNodesMIN, numNodesMAX, state, max_minutes):
    '''
    The rangeQuery() function creates a Pandas dataframe utilizing the user provided parameters to create statistical graphs. The graphs can be used to develop inferences in what is occuring in the dataset.
    This function differs from query() because it allows the user to input a range of nodes to query.
    :param connection: Object that holds the established SQL-Python connection to the database
    :param tableName: Object holding the table name the user is requesting. Can either be specified to one specific table or all tables in the database
    :param queueType: Object holding the requested queue type to be analyzed
    :param numNodesMIN: Object holding the minimum requested number of HPC nodes to be analyzed
    :param numNodesMAX: Object holding the maximum requested number of HPC nodes to be analyzed
    :param state: Object holding the requested job state to be analyzed
    :param max_minutes: Object holding the requested maximum amount of minutes a job can run for, utilized for specified analysis
    :return:
        Returns a specified Pandas dataframe, as well as corresponding graphs
    '''
    allcase = "All"
    #if tableName.casefold() == allcase.casefold():
    #else:

    query = ("SELECT jobid, user, account, TIMESTAMPDIFF(minute, submit, start) AS queueTime, TIMESTAMPDIFF(minute, start, end) AS runTime, queue, max_minutes, state, nnodes, reqcpus, nodelist"
         " FROM " + tableName + " WHERE queue LIKE '" + queueType + "' AND nnodes BETWEEN " + numNodesMIN + " AND " + numNodesMAX + " AND state LIKE '" + state + "' AND max_minutes BETWEEN 1 AND " + max_minutes + "")

    df = pd.read_sql(query, connection)
    print(df)

'''
    qTdf = df["queueTime"]

    summary = df["queueTime"].describe()
    summary['var'] = summary['std']**2.0
    print("Queue Time Summary\n", summary.apply(lambda x: format(x, 'f')))

    bp1 = df.boxplot(column = "queueTime", by = "max_minutes", grid = False)
    bp2 = df.boxplot(column = "queueTime", by = "queue", grid = False)
    bp3 = df.boxplot(column = "nnodes", by = "reqcpus", grid = False)
    #hist = qTdf.plot(kind = "hist", title = "Histogram of Queue Times for " + tableName)
    #hist.set_xlabel("Queue Time(sec")
    #hist.set_ylabel("Number of jobs")
    scatt = df.plot(kind = "scatter", grid = True, title = "Histogram of queue times with respect to max_minutes", x = "queueTime", y = "runTime")
    scatt.set_xlabel("Queue Time(sec)")
    scatt.set_ylabel("Run Time (sec)")
    plt.show()
'''
    # Filtering by types of jobs (partition, max minutes, nnodes)
    # Reqcpus
    # Terminal - Queue and nnode (support range [min, max] or exact) for all or a specific system
    # No need for a specified number of jobs, do above

def main():
    while len(sys.argv) != 6 and len(sys.argv) != 7:
        try:

            print("Please enter the correct amount of command-line arguments (5/6) in their respective order: "
                  "\npython3 HPCDataAnalysisTool.py [Table Name/All] [Queue Type] [Nnodes - Exact or Range] [State] [Max_Minutes]")
            sys.exit(1)
        except ValueError:
            print(
                "Incorrect number of arguments submitted, please make sure to enter the correct amount of command-line arguments (5/6) in their respective order: "
                "\npython3 HPCDataAnalysisTool.py [Table Name/All] [Queue Type] [Nnodes - Exact or Range] [State] [Max_Minutes]")

    if len(sys.argv) == 6:
        # For the case where the user inputs the exact number of nodes they would like to analyze rather than a range
        tableName = sys.argv[1]
        queueType = sys.argv[2]
        numNodes = sys.argv[3]
        state = sys.argv[4]
        max_minutes = sys.argv[5]
        connection = connect()
        query(connection, tableName, queueType, numNodes, state, max_minutes)

    if len(sys.argv) == 6:
        # For the case where the user inputs a range for the number of nodes they would like to analyze
        tableName = sys.argv[1]
        queueType = sys.argv[2]
        numNodesMIN = sys.argv[3]
        numNodesMAX = sys.argv[4]
        state = sys.argv[5]
        max_minutes = sys.argv[6]
        connection = connect()
        rangeQuery(connection, tableName, queueType, numNodesMIN, numNodesMAX, state, max_minutes)

    connection.close()
    sys.exit(1)

if __name__ == '__main__':
    main()
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
        table_queries = []
        for table in ["frontera", "lonestar6", "stampede", "stampede2", "maverick"]:
            table_queries.append(f"""
                    SELECT
            tablename.jobid,
            tablename.user,
            tablename.account,
            TIMESTAMPDIFF(minute, tablename.submit, tablename.start) AS queueTime,
            TIMESTAMPDIFF(minute, tablename.start, tablename.
            end
            ) AS runTime,
            tablename.queue,
            tablename.max_minutes,
            tablename.state,
            tablename.nnodes,
            tablename.reqcpus,
            tablename.nodelist,
            '{table}' as computer_source 
        FROM
            {table} 
        WHERE
            queue LIKE '" + queueType + "' 
            AND nnodes LIKE '" + numNodes + "' 
            AND state LIKE '" + state + "'
        """)

        query = "UNION".join(table_queries)
        query += "ORDER BY computer_source;"
    else:
        query = ("SELECT jobid, user, account, TIMESTAMPDIFF(second, submit, start) AS queueTime, TIMESTAMPDIFF(second, start, end) AS runTime, queue, max_minutes, state, nnodes, reqcpus, nodelist"
            " FROM " + tableName + " WHERE queue LIKE '" + queueType + "' AND nnodes =" + numNodes + " AND state LIKE '" + state + "' AND max_minutes BETWEEN 1 AND " + max_minutes + "")
        print(query)
    df = pd.read_sql(query, connection)
    df["queueTime"] = df["queueTime"] * 0.0166667 # Multiplies the "queueTime" column (QT) in the dataframe to convert the QT from seconds to minutes
    df["runTime"] = df["runTime"] * 0.0166667 # Multiplies the "runTime" column (RT) in the dataframe to convert the RT from seconds to minutes
    print(df)
    summary = df["queueTime"].describe()
    summary['var'] = summary['std'] ** 2.0
    print("Queue Time Summary\n ", summary.apply(lambda x: format(x, 'f')))
    #graphicalAnalysis(df)

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
    query = ("SELECT jobid, user, account, TIMESTAMPDIFF(second, submit, start) AS queueTime, TIMESTAMPDIFF(second, start, end) AS runTime, queue, max_minutes, state, nnodes, reqcpus, nodelist"
         " FROM " + tableName + " WHERE queue LIKE '" + queueType + "' AND nnodes BETWEEN " + numNodesMIN + " AND " + numNodesMAX + " AND state LIKE '" + state + "' AND max_minutes BETWEEN 1 AND " + max_minutes + "")
    #print(query)
    df = pd.read_sql(query, connection)

    df["queueTime"] = df["queueTime"] * 0.0166667 # Multiplies the "queueTime" column (QT) in the dataframe to convert the QT from seconds to minutes
    df["runTime"] = df["runTime"] * 0.0166667 # Multiplies the "runTime" column (RT) in the dataframe to convert the RT from seconds to minutes

    # This section between lines 139 and 144 finds both the minimum value and the index of the minimum value of the data frame
    # Which I used to manually go back into SQL to find the row that produced the first negative number
    print(df)
    s = df["queueTime"].idxmin()
    print(s)
    print(df.loc[[s]])
    t = df["queueTime"].min()
    print(t)

    # This section attempts to print out all negative values in the "queueTime" pandas column. This is achieved successfully,
    # but unsuccessfully inverts these values so they could be considered in the data set
    print(df[df["queueTime"] < 0]) #= (df.apply(lambda x: 1/x))
    #s = (df["queueTime"] < 0)
    #iv = s.apply(lambda x: 1/x)
    #print(iv)

    #t = df["queueTime"].min()
    #print(t)
    #summary = df["queueTime"].describe()
    #summary['var'] = summary['std'] ** 2.0
    #print("Queue Time Summary\n", summary.apply(lambda x: format(x, 'f')))
    #graphicalAnalysis(df)


def graphicalAnalysis(dataframe):
    '''
    The function graphicalAnalysis() creates plots based on specified objects considered key for analysis from function provided dataframes, which holds the user-specified queried data.
    :param dataframe: Pandas Dataframe object that holds the historical job data queried by the user
    :return:
        Returns 7 scatter plots detailing tendencies between job data points such as a jobs run time and the amount of CPU processing power requested by the job user.
    '''
    #bp1 = df.boxplot(column = "queueTime", by = "runTime", grid = False)
    #bp2 = df.boxplot(column = "queueTime", by = "queue", grid = False)
    #bp3 = df.boxplot(column = "nnodes", by = "reqcpus", grid = False)
    #hist = qTdf.plot(kind = "hist", title = "Histogram of Queue Times for " + tableName)
    #hist.set_xlabel("Queue Time(sec")
    #hist.set_ylabel("Number of jobs")


    # Scatter plot of Queue Time with respect to Run Time
    scatt1 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of queue time with respect to run time", x = "queueTime", y = "runTime")
    scatt1.set_xlabel("Queue Time (min)")
    scatt1.set_ylabel("Run Time (min)")

    # Scatter plot of Queue Time with respect to the number of Nodes requested
    scatt2 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of queue time with respect to the number of nodes requested", x = "queueTime", y = "nnodes")
    scatt2.set_xlabel("Queue Time (min)")
    scatt2.set_ylabel("Number of Nodes")

    # Scatter plot of Run Time with respect to the number of Nodes requested
    scatt3 = dataframe.plot(kind = "scatter", grid = True, title="Scatterplot of run time with respect to the number of nodes requested", x= "runTime", y="nnodes")
    scatt3.set_xlabel("Run Time (min)")
    scatt3.set_ylabel("Number of Nodes")

    # Scatter plot of Queue Time with respect to the number of CPUs requested
    scatt4 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of queue time with respect to the number of CPUs requested", x = "queueTime",y = "reqcpus")
    scatt4.set_xlabel("Queue Time (min)")
    scatt4.set_ylabel("Number of CPUs Requested")

    # Scatter plot of Run Time with respect to the number of Nodes requested
    scatt5 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of run time with respect to the number of CPUs requested", x = "runTime", y = "reqcpus")
    scatt5.set_xlabel("Run Time (min)")
    scatt5.set_ylabel("Number of CPUs Requested")

    # Scatter plot of Queue Time with respect to the Partition requested
    scatt6 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of queue time with respect to the Partition requested", x = "queueTime",y = "queue")
    scatt6.set_xlabel("Queue Time (min)")
    scatt6.set_ylabel("Partition Requested")

    # Scatter plot of Run Time with respect to the Partition requested
    scatt7 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of run time with respect to the Partition requested", x = "runTime", y = "queue")
    scatt7.set_xlabel("Run Time (min)")
    scatt7.set_ylabel("Partition Requested")

    plt.show()

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

    if len(sys.argv) == 7:
        # For the case where the user inputs a range for the number of nodes they would like to analyze
        tableName = sys.argv[1]
        queueType = sys.argv[2]
        numNodesMIN = sys.argv[3]
        numNodesMAX = sys.argv[4]
        state = sys.argv[5]
        max_minutes = sys.argv[6]
        connection = connect()
        rangeQuery(connection, tableName, queueType, numNodesMIN, numNodesMAX, state, max_minutes)

    connection.close() # 'Engine' object has no attribute 'close'
    sys.exit(1)

if __name__ == '__main__':
    main()
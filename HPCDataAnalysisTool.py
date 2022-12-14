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
my_passwd = "l0v31Zth#7890"  # Password for the user with the access mentioned on the line above
my_database = "HPC_Job_Database"  # The MySQL variable that hosts the name of the database that the tables of the submitted data will be stored on (Variable name to change at discretion of user)
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

def query(connection, tableName, queueType, numNodes, state, max_minutes, qtRange, rtRange):
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
    '''
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
    '''
    query = ("SELECT jobid, user, account, TIMESTAMPDIFF(second, submit, start) AS queueTime, TIMESTAMPDIFF(second, start, end) AS runTime, queue, max_minutes, state, nnodes, reqcpus, nodelist"
        " FROM " + tableName + " WHERE queue = '" + queueType + "' AND nnodes =" + numNodes + " AND state = '" + state + "' AND max_minutes BETWEEN 1 AND " + max_minutes + "")
    print(query)
    df = pd.read_sql(query, connection)
    df["queueTime"] = df["queueTime"] * 0.0166667 # Multiplies the "queueTime" column (QT) in the dataframe to convert the QT from seconds to minutes
    df["runTime"] = df["runTime"] * 0.0166667 # Multiplies the "runTime" column (RT) in the dataframe to convert the RT from seconds to minutes
    positiveQTDFtemp = df[df["queueTime"] >= 0].copy()
    positiveRTDFtemp = df[df["runTime"] >= 0].copy()
    correctedDF = pd.merge(positiveQTDFtemp, positiveRTDFtemp, how="inner")

    datetimeErrors = len(df.index) - len(correctedDF.index)
    print("\nThere are a total of", datetimeErrors, "errors in the dataset, which are highlighted below:\n")

    negativeQTDF = df[df["queueTime"] < 0].copy()
    negativeRTDF = df[df["runTime"] < 0].copy()
    print("Negative QTs Found:\n", negativeQTDF, "\n")
    print("Negative RTs Found:\n", negativeRTDF, "\n")

    summary = correctedDF["queueTime"].describe()
    summary['var'] = summary['std'] ** 2.0
    print("Corrected Queue Time Summary")
    print(summary.apply(lambda x: format(x, 'f')))
    graphicalAnalysis(correctedDF)

def rangeQuery(connection, tableName, queueType, numNodesMIN, numNodesMAX, state, max_minutes, qtRange, rtRange):
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
    QTrange = int(qtRange)
    RTrange = int(rtRange)

    allcase = "All"
    #if tableName.casefold() == allcase.casefold():
    #else:
    query = ("SELECT jobid, user, account, TIMESTAMPDIFF(second, submit, start) AS queueTime, TIMESTAMPDIFF(second, start, end) AS runTime, queue, max_minutes, state, nnodes, reqcpus, nodelist"
         " FROM " + tableName + " WHERE queue = '" + queueType + "' AND nnodes BETWEEN " + numNodesMIN + " AND " + numNodesMAX + " AND state = '" + state + "' AND max_minutes BETWEEN 1 AND " + max_minutes + "")
    print(query)
    df = pd.read_sql(query, connection)

    df["queueTime"] = df["queueTime"] * 0.0166667 # Multiplies the "queueTime" column (QT) in the dataframe to convert the QT from seconds to minutes
    df["runTime"] = df["runTime"] * 0.0166667 # Multiplies the "runTime" column (RT) in the dataframe to convert the RT from seconds to minutes

    # This section between lines 139 and 144 finds both the minimum value and the index of the minimum value of the data frame
    # Which I used to manually go back into SQL to find the row that produced the first negative number
    #print("Original", df)
    #s = df["queueTime"].idxmin()
    #print(s)
    #print(df.loc[[s]])
    #i = df["queueTime"].min()
    #print("queuetime min for orig", i)
    #l = df["runTime"].min()
    #print("runtime min for orig", l)


    # The objective is to 1) make a copy of the main dataframe where the QT or the RT is < 0, remove those elements that make up those copies from the main DF and then graph each case

    positiveQTDFtemp = df[df["queueTime"] >= 0].copy()
    positiveRTDFtemp = df[df["runTime"] >= 0].copy()
    correctedDF = pd.merge(positiveQTDFtemp, positiveRTDFtemp, how = "inner" )
    print(correctedDF)
    #print("Corrected\n", correctedDF) # check to see that it truly was corrected
    #t = correctedDF["queueTime"].min()
    #print("queuetime min", t)
    #p = correctedDF["runTime"].min()
    #print("runtime min", p)

    datetimeErrors = len(df.index) - len(correctedDF.index)
    print("\nThere are a total of", datetimeErrors, "errors in the dataset, which are highlighted below:\n")

    negativeQTDF = df[df["queueTime"] < 0].copy()
    negativeRTDF = df[df["runTime"] < 0].copy()
    print("Negative QTs Found:\n", negativeQTDF, "\n")
    print("Negative RTs Found:\n", negativeRTDF, "\n")

    '''
        result = df["queueTime"].describe()
        result['var'] = result['std'] ** 2.0
        print("Original Queue Time Summary")
        print(result.apply(lambda x: format(x, 'f')))
    
    
    '''

    timelimitSummary = correctedDF["max_minutes"].describe()
    timelimitSummary['var'] = timelimitSummary['std'] ** 2.0
    timelimitSummary['median'] = correctedDF["queueTime"].median()
    print("Requested Max Time Limit Summary")
    print(timelimitSummary.apply(lambda x: format(x, 'f')))

    queueSummary = correctedDF["queueTime"].describe()
    queueSummary['var'] = queueSummary['std'] ** 2.0
    queueSummary['median'] = correctedDF["queueTime"].median()
    print("\nQueue Time Summary")
    print(queueSummary.apply(lambda x: format(x, 'f')))

    graphicalAnalysis(correctedDF)

    '''
    longQT = correctedDF[(correctedDF["queueTime"] >= QTrange) & (correctedDF["runTime"] <= RTrange)].copy()
        longQTsum = longQT["queueTime"].describe()
        longQTsum['var'] = longQTsum['std'] ** 2.0
        print("Adjusted Queue Time Summary for the queue bounds of " + qtRange + " and run bounds of " + rtRange)
        print(longQTsum.apply(lambda x: format(x, 'f')))
        #graphicalAnalysis(correctedDF)
        graphicalAnalysis(longQT)
    
    '''


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

    # Scatter plot of Time Limit requested to number of Nodes requested
    scatt1 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of maximum job run time limit requested with respect to number of Nodes requested", x = "max_minutes", y = "nnodes")
    scatt1.set_xlabel("Time Limit/Max_minutes Requested (min)")
    scatt1.set_ylabel("Number of Nodes Requested")

    # Scatter plot of Time Limit requested with respect to the number of CPUs requested
    scatt2 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of maximum job run time limit requested with respect to the number of CPUs requested", x = "max_minutes", y = "reqcpus")
    scatt2.set_xlabel("Time Limit/Max_minutes Requested (min)")
    scatt2.set_ylabel("Number of CPUs Requested")

    # Scatter plot of Queue Time with respect to Time Limit requested
    scatt3 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of queue time with respect to maximum job run time limit requested", x = "max_minutes", y = "queueTime")
    scatt3.set_xlabel("Time Limit/Max_minutes Requested (min)")
    scatt3.set_ylabel("Queue Time (min)")

    # Scatter plot of Queue Time with respect to the number of Nodes requested
    scatt4 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of queue time with respect to the number of nodes requested", x = "nnodes", y = "queueTime")
    scatt4.set_xlabel("Number of Nodes Requested")
    scatt4.set_ylabel("Queue Time (min)")

    # Scatter plot of Queue Time with respect to the number of CPUs requested
    scatt5 = dataframe.plot(kind = "scatter", grid = True, title = "Scatterplot of queue time with respect to the number of CPUs requested", x = "reqcpus", y = "queueTime")
    scatt5.set_xlabel("Number of CPUs Requested")
    scatt5.set_ylabel("Queue Time (min)")

    plt.show()

def main():
    while len(sys.argv) != 7 and len(sys.argv) != 8:
        try:

            print("Please enter the correct amount of command-line arguments (7/8) in their respective order: "
                  "\npython3 HPCDataAnalysisTool.py [Table Name/All] [Queue Type] [Nnodes - Exact or Range] [State] [Max_Minutes] [Queue time range to be observed]")
            sys.exit(1)
        except ValueError:
            print(
                "Incorrect number of arguments submitted, please make sure to enter the correct amount of command-line arguments (7/8) in their respective order: "
                "\npython3 HPCDataAnalysisTool.py [Table Name/All] [Queue Type] [Nnodes - Exact or Range] [State] [Max_Minutes] [Queue time range to be observed]")

    if len(sys.argv) == 7:
        # For the case where the user inputs the exact number of nodes they would like to analyze rather than a range
        tableName = sys.argv[1]
        queueType = sys.argv[2]
        numNodes = sys.argv[3]
        state = sys.argv[4]
        max_minutes = sys.argv[5]
        qtRange = sys.argv[6]
        connection = connect()
        query(connection, tableName, queueType, numNodes, state, max_minutes, qtRange)

    if len(sys.argv) == 8:
        # For the case where the user inputs a range for the number of nodes they would like to analyze
        tableName = sys.argv[1]
        queueType = sys.argv[2]
        numNodesMIN = sys.argv[3]
        numNodesMAX = sys.argv[4]
        state = sys.argv[5]
        max_minutes = sys.argv[6]
        qtRange = sys.argv[7]
        connection = connect()
        rangeQuery(connection, tableName, queueType, numNodesMIN, numNodesMAX, state, max_minutes, qtRange)

    #connection.close() # 'Engine' object has no attribute 'close'
    sys.exit(1)

if __name__ == '__main__':
    main()
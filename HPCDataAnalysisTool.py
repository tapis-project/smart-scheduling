from __future__ import print_function
import sys
import pandas as pd
import sqlalchemy as sa
import mysql.connector
import matplotlib.pyplot as plt

from mpl_toolkits.mplot3d import axes3d
from numpy import float32, uint32
import numpy as np
from datetime import timedelta, datetime
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
NORMALQUEUE_MAXTIME = 2880
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

    genConnection = mysql.connector.connect(host=my_host, user=my_user, passwd=my_passwd, database=my_database)

    #print('\nSuccessfully Connected to your MySQL Database:', my_database)

    return genConnection

def query(connection, mainConnection, tableName):
    '''
    jobid = '7257293'

    # This code block is the initial sql query, which finds for some jobid it's job information in the stampede2_jobq table
    initialQuery = "SELECT * FROM " + tableName + " WHERE jobid = '" + jobid + "'"
    print(initialQuery)
    df = pd.read_sql(initialQuery, connection)
    #print("dataframe:\n", df)

    # Taking that job id's data, I grab it's submit time and query all the jobs that are in the backlog
    initialQuery_SubmitTime = df["submit"].dt.strftime("%Y-%m-%d %H:%M:%S")
    submit = initialQuery_SubmitTime[0]
    initialQuery_BacklogMin = df["backlog_minutes"]

    backlog_minutes = initialQuery_BacklogMin[0]
    #print(backlog_minutes)

    clarifyingQuery = "SELECT * FROM " + tableName + " WHERE start > '" + submit + "' and submit < '" + submit + "'"
    print(clarifyingQuery)
    backlogDF = pd.read_sql(clarifyingQuery, connection)
    #print("backlogDF:\n", backlogDF)



    # From here, we iterate through the backlog df to develop a weight formula to predict the queues_minutes a job may experience

    backlog_maxmin = backlogDF["max_minutes"]
    backlog_submit = backlogDF["submit"]
    print("BACKLOG SUBMIT\n", backlog_submit)
    avgSubmitTimeInterval = 0


    for i in range(1, len(backlog_submit.index)):
        print("i\n", type(backlog_submit[i]))
        bljS1 = datetime.fromtimestamp(backlog_submit[i], tz = None)#.strftime("%Y-%m-%d %H:%M:%S")
        #bljS2 = datetime.fromtimestamp(backlog_submit[i-1])

        print("bljs1\n", bljS1)
        #avgSubmitTimeInterval += bljS1 - bljS2
        #print("avg:\n", avgSubmitTimeInterval)
        break
    totalBacklogMinTMP = 0
    calc1 = 0
    totalnumBackedupJobs = len(backlog_maxmin.index)

    for i in range(0, len(backlog_maxmin.index)):
        currentblj_maxmin = backlog_maxmin[i]

        totalBacklogMinTMP += currentblj_maxmin # working and correct



    blSum = np.float32(totalBacklogMinTMP)

    totalBacklogMin = blSum.item()
    denominator = (NORMALQUEUE_MAXTIME * totalnumBackedupJobs)
    calc1 = (totalBacklogMin / (NORMALQUEUE_MAXTIME * totalnumBackedupJobs)) * denominator

    print("\nTotal Number of Backed up Jobs: ", totalnumBackedupJobs)
    print("\nTotal Amount of Time Requested in the Backlog: ", totalBacklogMin)
    print("\nNormal Queue Max Time a job can request (2880) times the total number of backed up jobs: ", denominator)
    print("\nCalculation 1: total number of time requested back up jobs / max time you could wait (IE 2880 * total number of backed up jobs", calc1 )
    '''

    connection = connection
    mainConnection = mainConnection

    cursor = mainConnection.cursor()
    n = 5 # group number -> will be added by itself IE. 5, 10, 15, ...

    '''
    maxBacklog = "SELECT MAX(backlog_num_jobs) FROM HPC_Job_Database.stampede2_jobq;"
    cursor.execute(maxBacklog)
    maxBacklog = cursor.fetchall() # maxBacklog is a list
    maxBacklog = maxBacklog[0][0] # Now a tuple

    classQuery = "SELECT * FROM HPC_Job_Database.stampede2_jobq WHERE backlog_num_jobs = "
    
    classNumList = []
    classMeanList = []
    classStdList = []
    
    
    for i in range(n, maxBacklog, 5):
        
        print("Backlog Class: ", i)
        classQuerytmp = classQuery + str(i) + ";"

        df = pd.read_sql(classQuerytmp, connection)
        #submit = df["submit"]
        #start = df["start"]

        #df["Queue Time"] = (start - submit).dt.seconds
        queuetime = df["queue_minutes"]
        
        queuetimeMean = float(queuetime.mean())
        queuetimeStd = float(queuetime.std())

        queuetimeMeanPR = convert(queuetime.mean())
        queuetimeStdPR = convert(queuetime.std())
        #print("Mean queue time: " + str(queuetimeMean))
        #print("Standard deviation: " + str(queuetimeStd) + "\n--------------------------------------")
        classNumList.append(i)
        classMeanList.append(queuetimeMean)
        classStdList.append(queuetimeStd)

    classMeanMAX = max(classMeanList)
    classMeanMIN = min(classMeanList)

    classMeanMaxIndex = classMeanList.index(classMeanMAX)
    classMeanMinIndex = classMeanList.index(classMeanMIN)

    classMeanMaxIndexNumber = classNumList[classMeanMaxIndex]
    classMeanMinIndexNumber = classNumList[classMeanMinIndex]

    print("Class Min Max for Class number " + str(classMeanMaxIndexNumber) + " -> " + str(classMeanMAX))
    print("Class Mean Min for Class number " + str(classMeanMinIndexNumber) + " -> " + str(classMeanMIN))
    '''
    #----------------------------------------------------------

    backlog_n = 0  # backlog group number -> iterate by 10s

    backlog_minutesMAX_tmp = "SELECT MAX(backlog_minutes) FROM HPC_Job_Database.stampede2_jobq;"
    cursor.execute(backlog_minutesMAX_tmp)
    backlog_minutesMAX_tmp = cursor.fetchall()  # max Backlog minutes is a list
    backlog_minutesMAX = backlog_minutesMAX_tmp[0][0]  # Now a tuple

    genBacklogMinQuery = "SELECT * FROM HPC_Job_Database.stampede2_jobq WHERE backlog_minutes = " # <- queue_minutes and between a range(0, i)

    backlog_minutesList = []
    backlog_minutesMeanList = []
    backlog_minutesStdList = []

    for i in range(backlog_n, backlog_minutesMAX, 100):
        #print("Current backlog minute =", i)
        backlogQuerytmp = genBacklogMinQuery + str(i) + ";"

        df = pd.read_sql(backlogQuerytmp, connection)

        queuetime = df["queue_minutes"]

        queuetimeMean = float(queuetime.mean())
        queuetimeStd = float(queuetime.std())

        # queuetimeMeanPR = convert(queuetime.mean())
        # queuetimeStdPR = convert(queuetime.std())
        # print("Mean queue time: " + str(queuetimeMean))
        # print("Standard deviation: " + str(queuetimeStd) + "\n--------------------------------------")

        backlog_minutesList.append(i)
        backlog_minutesMeanList.append(queuetimeMean)
        backlog_minutesStdList.append(queuetimeStd)

    backlogNum = backlog_minutesList
    backlogMinMean = backlog_minutesMeanList
    backlogStd = backlog_minutesStdList

    TenPercentQTSTD = []
    TenPercentQTSTD_minStandVal = []

    FivePercentQTSTD = []
    FivePercentQTSTD_minStandVal = []

    FifteenPercentQTSTD = []
    FifteenPercentQTSTD_minStandVal = []

    TwentyPercentQTSTD = []
    TwentyPercentQTSTD_minStandVal = []

    # 10% Standard Deviation

    for i in range(len(backlogStd)):
        if backlogMinMean[i] == 0:
            continue
        if backlogStd[i] <= (0.1 * backlogMinMean[i]):
            TenPercentQTSTD.append(i)

    for i in range(len(TenPercentQTSTD)):
        tmp = TenPercentQTSTD[i]
        tmp2 = backlogNum[tmp]
        TenPercentQTSTD_minStandVal.append(tmp2)

    # 5% Standard Deviation

    for i in range(len(backlogStd)):
        if backlogMinMean[i] == 0:
            continue
        if backlogStd[i] <= (0.05 * backlogMinMean[i]):
            FivePercentQTSTD.append(i)

    for i in range(len(FivePercentQTSTD)):
        tmp = FivePercentQTSTD[i]
        tmp2 = backlogNum[tmp]
        FivePercentQTSTD_minStandVal.append(tmp2)

    TenPercentQTSTDFIN = list(set(TenPercentQTSTD) ^ set(FivePercentQTSTD))
    TenPercentQTSTD_minStandValFIN = list(set(TenPercentQTSTD_minStandVal) ^ set(FivePercentQTSTD_minStandVal))

    # 15% Standard Deviation

    for i in range(len(backlogStd)):
        if backlogMinMean[i] == 0:
            continue
        if backlogStd[i] <= (0.15 * backlogMinMean[i]):
            FifteenPercentQTSTD.append(i)

    for i in range(len(FifteenPercentQTSTD)):
        tmp = FifteenPercentQTSTD[i]
        tmp2 = backlogNum[tmp]
        FifteenPercentQTSTD_minStandVal.append(tmp2)

    FifteenPercentQTSTDFIN = list(set(FifteenPercentQTSTD) ^ set(TenPercentQTSTD))
    FifteenPercentQTSTD_minStandValFIN = list(set(FifteenPercentQTSTD_minStandVal) ^ set(TenPercentQTSTD_minStandVal))

    # 20% Standard Deviation

    for i in range(len(backlogStd)):
        if backlogMinMean[i] == 0:
            continue
        if backlogStd[i] <= (0.20 * backlogMinMean[i]):
            TwentyPercentQTSTD.append(i)

    for i in range(len(TwentyPercentQTSTD)):
        tmp = TwentyPercentQTSTD[i]
        tmp2 = backlogNum[tmp]
        TwentyPercentQTSTD_minStandVal.append(tmp2)

    TwentyPercentQTSTDFIN = list(set(TwentyPercentQTSTD) ^ set(FifteenPercentQTSTD))
    TwentyPercentQTSTD_minStandValFIN = list(set(TwentyPercentQTSTD_minStandVal) ^ set(FifteenPercentQTSTD_minStandVal))







    print("Jobs Queue Minutes for standard deviation <= 5%", FivePercentQTSTD_minStandVal)
    print("Standard deviation (5%)", FivePercentQTSTD)

    print("Jobs Queue Minutes for standard deviation 5 <= 10% (Excluding 5%)", TenPercentQTSTD_minStandValFIN)
    print("Standard deviation (10%)", TenPercentQTSTDFIN)

    print("Jobs Queue Minutes for standard deviation 10 <= 15% (Excluding 5%)", FifteenPercentQTSTD_minStandValFIN)
    print("Standard deviation (15%)", FifteenPercentQTSTDFIN)

    #standardDeviationAnalysis(cursor, FivePercentQTSTD_minStandVal, TenPercentQTSTD_minStandValFIN, FifteenPercentQTSTD_minStandValFIN)
    #plot(connection, backlog_minutesList, backlog_minutesMeanList, backlog_minutesStdList)



    print("\nAnalysis tool start up...")
    genBacklogMinQuery = "SELECT COUNT(*) FROM HPC_Job_Database.stampede2_jobq WHERE backlog_minutes = "

    print("-----------------------------------------------------------------", "\nFive Percent Standard Deviation Queue Minutes")

    for i in range(len(FivePercentQTSTD_minStandVal)):
        ithBacklogMin = FivePercentQTSTD_minStandVal[i]
        backlogQuerytmp = genBacklogMinQuery + str(ithBacklogMin) + ";"
        df = pd.read_sql(backlogQuerytmp, connection)
        print(backlogQuerytmp, "\nResult:\n", df, "\n")

    print("-----------------------------------------------------------------", "\nTen Percent Standard Deviation Queue Minutes")

    for i in range(len(TenPercentQTSTD_minStandValFIN)):
        ithBacklogMin = TenPercentQTSTD_minStandValFIN[i]
        backlogQuerytmp = genBacklogMinQuery + str(ithBacklogMin) + ";"
        df = pd.read_sql(backlogQuerytmp, connection)
        print(backlogQuerytmp, "\nResult:\n", df, "\n")

    print("-----------------------------------------------------------------", "\nFifteen Percent Standard Deviation Queue Minutes")

    for i in range(len(FifteenPercentQTSTD_minStandValFIN)):
        ithBacklogMin = FifteenPercentQTSTD_minStandValFIN[i]
        backlogQuerytmp = genBacklogMinQuery + str(ithBacklogMin) + ";"
        df = pd.read_sql(backlogQuerytmp, connection)
        print(backlogQuerytmp, "\nResult:\n", df, "\n")

    print("-----------------------------------------------------------------",
          "\nTwenty Percent Standard Deviation Queue Minutes")

    for i in range(len(TwentyPercentQTSTD_minStandValFIN)):
        ithBacklogMin = TwentyPercentQTSTD_minStandValFIN[i]
        backlogQuerytmp = genBacklogMinQuery + str(ithBacklogMin) + ";"
        df = pd.read_sql(backlogQuerytmp, connection)
        print(backlogQuerytmp, "\nResult:\n", df, "\n")

    print("-----------------------------------------------------------------")


'''
def backlogMinQuery(connection, mainConnection):
    backlog_n = 0  # backlog group number -> iterate by 10s 
    cursor = mainConnection.cursor()

    backlog_minutesMAX_tmp = "SELECT MAX(backlog_minutes) FROM HPC_Job_Database.stampede2_jobq;"
    cursor.execute(backlog_minutesMAX_tmp)
    backlog_minutesMAX_tmp = cursor.fetchall()  # max Backlog minutes is a list
    backlog_minutesMAX = backlog_minutesMAX_tmp[0][0]  # Now a tuple

    genBacklogMinQuery = "SELECT * FROM HPC_Job_Database.stampede2_jobq WHERE backlog_minutes = "

    backlog_minutesList = []
    backlog_minutesMeanList = []
    backlog_minutesStdList = []

    for i in range(backlog_n, backlog_minutesMAX, 10):
        backlogQuerytmp = genBacklogMinQuery + str(i) + ";"

        df = pd.read_sql(backlogQuerytmp, connection)
        
        queuetime = df["queue_minutes"]

        queuetimeMean = float(queuetime.mean())
        queuetimeStd = float(queuetime.std())

        #queuetimeMeanPR = convert(queuetime.mean())
        #queuetimeStdPR = convert(queuetime.std())
        # print("Mean queue time: " + str(queuetimeMean))
        # print("Standard deviation: " + str(queuetimeStd) + "\n--------------------------------------")
        
        backlog_minutesList.append(i)
        backlog_minutesMeanList.append(queuetimeMean)
        backlog_minutesStdList.append(queuetimeStd)

    classMeanMAX = max(classMeanList)
    classMeanMIN = min(classMeanList)

    classMeanMaxIndex = classMeanList.index(classMeanMAX)
    classMeanMinIndex = classMeanList.index(classMeanMIN)

    classMeanMaxIndexNumber = classNumList[classMeanMaxIndex]
    classMeanMinIndexNumber = classNumList[classMeanMinIndex]

    print("Class Min Max for Class number " + str(classMeanMaxIndexNumber) + " -> " + str(classMeanMAX))
    print("Class Mean Min for Class number " + str(classMeanMinIndexNumber) + " -> " + str(classMeanMIN))
'''
def plot(mainConnection,  a, b, c):

    fig = plt.figure()
    #ax = fig.add_subplot(111, projection='3d')

    #classNum = x
    #meanQT = y
    #QTStd = z

    backlogNum = a
    backlogMinMean = b
    backlogStd = c



    '''
    ax.scatter(classNum, meanQT, QTStd, c='r', marker='o')

    ax.set_xlabel('Class Number')
    ax.set_ylabel('Mean QT (sec)')
    ax.set_zlabel('QT Standard Deviation')
    plt.title("3D Plot of Class Data of Step Size of 5")

    fig2 = plt.figure()
    plt.plot(classNum, meanQT)
    plt.title("Class Number vs Mean QT")
    plt.xlabel("Class Number")
    plt.ylabel("Mean Queue Time")

    fig3 = plt.figure()
    plt.plot(classNum, QTStd)
    plt.title("Class Number vs QT Standard Deviation")
    plt.xlabel("Class Number")
    plt.ylabel("Queue Time Standard Deviation")

    #

    classQuery = "SELECT * FROM HPC_Job_Database.stampede2_jobq WHERE backlog_num_jobs = 400"
    df = pd.read_sql(classQuery, connection)
    queuetime = df["queue_minutes"]

    fig4 = plt.figure()
    plt.plot(queuetime.index, queuetime.values)
    plt.title("Class Number vs QT for Class Num = 400")
    plt.xlabel("Class Number")
    plt.ylabel("Queue Time")

    '''

    '''
    fig5 = plt.figure()
    plt.plot(backlogNum, backlogMinMean)
    plt.title("Backlog Minutes vs Backlog Minutes Mean")
    plt.xlabel("Backlog Minutes (min)")
    plt.ylabel("BL Mean")

    fig6 = plt.figure()
    plt.plot(backlogNum, backlogStd)
    plt.title("Backlog Minutes vs BL Standard Deviation for all Jobs with increasing BL Min += 100")
    plt.xlabel("Backlog Minutes (min)")
    plt.ylabel("BL Standard Deviation")

    

    fig7 = plt.figure()
    for x, y in zip(FifteenPercentQTSTD_minStandVal, FifteenPercentQTSTD):
        plt.scatter(x, y, cmap="copper")
    plt.title("Jobs Where Their Backlog Queue time Standard Deviation is 15% from the Mean")
    plt.xlabel("Backlog Minutes (min)")
    plt.ylabel("BL Standard Deviation")

    plt.show()
    '''
    #standardDeviationAnalysis(mainConnection, FivePercentQTSTD_minStandVal, TenPercentQTSTD_minStandValFIN, FifteenPercentQTSTD_minStandValFIN)
def convert(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    return "%d:%02d:%02d" % (hour, minutes, seconds)

'''
def standardDeviationAnalysis(connection, FivePercentQueueMin, TenPercentQueueMin, FifteenPercentQueueMin):

    print("\nAnalysis tool start up...")
    genBacklogMinQuery = "SELECT COUNT(*) FROM HPC_Job_Database.stampede2_jobq WHERE backlog_minutes = "

    for i in range(len(FivePercentQueueMin)):
        ithBacklogMin = FivePercentQueueMin[i]
        backlogQuerytmp = genBacklogMinQuery + str(ithBacklogMin) + ";"
        df = pd.read_sql(backlogQuerytmp, connection)
        print(backlogQuerytmp, "\nResult:\n", df)

'''





def main():
    while len(sys.argv) != 2:
        try:

            print("Please enter the correct amount of command-line arguments in the respective order: "
                  "\npython3 HPCDataAnalysisTool.py [Table Name]")
            sys.exit(1)
        except ValueError:
            print("Incorrect number of arguments submitted, please make sure to enter the correct amount of command-line arguments (2) in their respective order: "
                "\npython3 HPCDataAnalysisTool.py [Table Name]")

    if len(sys.argv) == 2:
        connection = connect() # Connect via Pandas-SQL library
        mainConnection = connectGen() # mysql.connector connection
        tableName = sys.argv[1]
        query(connection, mainConnection, tableName)
        #tempPlot(connection, mainConnection, tableName)
    #connection.close() # 'Engine' object has no attribute 'close'
    sys.exit(1)

if __name__ == '__main__':
    main()
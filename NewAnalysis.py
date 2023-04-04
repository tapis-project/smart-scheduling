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

def convert(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    return "%d:%02d:%02d" % (hour, minutes, seconds)


def query(connection, mainConnection, tableName):


    connection = connection
    mainConnection = mainConnection

    cursor = mainConnection.cursor()
    '''
    "SELECT COUNT(max_minutes) AS TotalJobs," \
                "AVG(max_minutes) AS AverageMaxMinutesRequestedPerJob," \
                "AVG(queue_minutes) AS AverageQueueMinutesEachJobExperienced," \
                "AVG(backlog_minutes) AS AverageBacklogMinutes," \
                "AVG(backlog_num_jobs) AS AverageNumberOfBacklogJobs," \
                "STD(max_minutes) AS STDFORAverageMaxMinutesRequestedPerJob," \
                "STD(queue_minutes) AS STDFORAverageQueueMinutesEachJobExperienced," \
                "STD(backlog_minutes) AS STDFORAverageBacklogMinutes," \
                "STD(backlog_num_jobs) AS STDFORAverageNumberOfBacklogJobs" \
                " FROM HPC_Job_Database.stampede2_jobq" \
                " WHERE max_minutes BETWEEN 0 AND " + str(MAX_MINUTESMAX) + \
            " AND backlog_minutes BETWEEN 0 AND " + str(BACKLOG_MINUTESMAX) + \
            " AND backlog_num_jobs BETWEEN 0 and " + str(BACKLOG_NUM_JOBSMAX) + \
            " AND queue_minutes BETWEEN 0 AND " + str(QUEUE_MINUTESMAX) + ";".format(i, i + 60)
    '''

    total_jobs_list = []
    average_max_minutes_list = []
    average_queue_minutes_list = []
    average_backlog_minutes_list = []
    average_number_of_backlog_jobs_list = []
    std_for_average_max_minutes_list = []
    std_for_average_queue_minutes_list = []
    std_for_average_backlog_minutes_list = []
    std_for_average_number_of_backlog_jobs_list = []
    queueMin_MaxMinutesMean = []
    queueMin_MaxMinutesStanDev = []

    
    # Max column values in the stampede2_jobq dataset 
    NORMALQUEUE_MAXTIME = 2880 # <- make smaller names, still be descriptive 
    MAX_MINUTESMAX = 10080
    BACKLOG_MINUTESMAX = 2562394
    BACKLOG_NUM_JOBSMAX = 1518
    QUEUE_MINUTESMAX = 90116

    # "Knobs" for the query adjustment
    loop_step_size = 60 # Knobs for for-loop range() function 
    start_bound_ForLoop = 0
    end_bound_ForLoop = MAX_MINUTESMAX
    
    STRTMAXMIN = "0" 
    ENDMAXMIN = "120"
    
    STRTBKLGMIN = "0"
    ENDBKLGMIN = "120"
    
    STRTBKLGNUMJOBS = "0"
    ENDBKLGNUMJOBS = "120"

    STRTQUEUEMIN = "0"
    ENDQUEUEMIN = "120"

    with open('Max Minutes Data Analysis.txt', "w") as f:
        for i in range(start_bound_ForLoop, end_bound_ForLoop, loop_step_size):
            maxMinquery = "SELECT COUNT(max_minutes) AS TotalJobs," \
                          "AVG(max_minutes) AS AverageMaxMinutesRequestedPerJob," \
                          "AVG(queue_minutes) AS AverageQueueMinutesEachJobExperienced," \
                          "AVG(backlog_minutes) AS AverageBacklogMinutes," \
                          "AVG(backlog_num_jobs) AS AverageNumberOfBacklogJobs," \
                          "STD(max_minutes) AS STDFORAverageMaxMinutesRequestedPerJob," \
                          "STD(queue_minutes) AS STDFORAverageQueueMinutesEachJobExperienced," \
                          "STD(backlog_minutes) AS STDFORAverageBacklogMinutes," \
                          "STD(backlog_num_jobs) AS STDFORAverageNumberOfBacklogJobs" \
                          " FROM HPC_Job_Database.stampede2_jobq" \
                          " WHERE max_minutes BETWEEN " + STRTMAXMIN + " AND " + ENDMAXMIN + \
                          " AND backlog_minutes BETWEEN " + STRTBKLGMIN + " AND " + ENDBKLGMIN + \
                          " AND backlog_num_jobs BETWEEN " + STRTBKLGNUMJOBS + " AND " + ENDBKLGNUMJOBS + \
                          " AND queue_minutes BETWEEN " + STRTQUEUEMIN + " AND " + ENDQUEUEMIN + ";"

            # str(i) + " AND " + str(i + loop_step_size)
            df_tmp = pd.read_sql(maxMinquery, connection)

            #print("query", maxMinquery)
            #print("\n",df)

            df = df_tmp.fillna(0) # Handles cases where the dataframe returns "NoneType" values because there is no data that is returned and as such replaces all those values with 0
            # Method not full proof, needs to be handled better to skip AND statements where that occurs

            total_jobs = float(df["TotalJobs"])
            average_max_minutes = float(df["AverageMaxMinutesRequestedPerJob"])
            average_queue_minutes = float(df["AverageQueueMinutesEachJobExperienced"])
            average_backlog_minutes = float(df["AverageBacklogMinutes"])
            average_number_of_backlog_jobs = float(df["AverageNumberOfBacklogJobs"])
            std_for_average_max_minutes = float(df["STDFORAverageMaxMinutesRequestedPerJob"])
            std_for_average_queue_minutes = float(df["STDFORAverageQueueMinutesEachJobExperienced"])
            std_for_average_backlog_minutes = float(df["STDFORAverageBacklogMinutes"])
            std_for_average_number_of_backlog_jobs = float(df["STDFORAverageNumberOfBacklogJobs"])

            total_jobs_list.append(total_jobs)
            average_max_minutes_list.append(average_max_minutes)
            average_queue_minutes_list.append(average_queue_minutes)
            average_backlog_minutes_list.append(average_backlog_minutes)
            average_number_of_backlog_jobs_list.append(average_number_of_backlog_jobs)
            std_for_average_max_minutes_list.append(std_for_average_max_minutes)
            std_for_average_queue_minutes_list.append(std_for_average_queue_minutes)
            std_for_average_backlog_minutes_list.append(std_for_average_backlog_minutes)
            std_for_average_number_of_backlog_jobs_list.append(std_for_average_number_of_backlog_jobs)

            queueMin_MaxMinutesMean.append((average_max_minutes + average_queue_minutes) / 2)
            queueMin_MaxMinutesStanDev.append(np.std([average_max_minutes, average_queue_minutes]))

            percentGain = [100 * (queueMin_MaxMinutesStanDev[i] / queueMin_MaxMinutesMean[i]) for i in
                           range(len(queueMin_MaxMinutesMean))]

            # print the last element of queueMin_MaxMinutesMean
            print("Current WHERE CLAUSE: " + str(i) + " AND " + str(i + 60))

            #print("Total Number of Jobs: ", total_jobs)

            #print("Current value of average max_minutes:", average_max_minutes)

            # print the last element of queueMin_MaxMinutesMean
            #print("Current value of average queue_minutes:", average_queue_minutes)

            #print("Mean between average_max_minutes and average_queue_minutes:", queueMin_MaxMinutesMean[-1])

            #print("Standard Deviation between average_max_minutes and average_queue_minutes:", queueMin_MaxMinutesStanDev[-1])

            #print("Percent Gain", percentGain[-1], "\n")

            if average_queue_minutes > average_max_minutes:
                print("Found Potential For Gain at AND statement -> " + str(i) + " AND " + str(i + 60))
                f.write("Found Potential For Gain at AND statement -> " + str(i) + " AND " + str(i + 60))
                f.write("Total Number of Jobs: " +  str(total_jobs) + "\n")
                f.write("Current WHERE CLAUSE: " + str(i) + " AND " + str(i + 60) + "\n")
                f.write("Current value of average max_minutes: " + str(average_max_minutes) + "\n")
                f.write("Current value of average queue_minutes: " + str(average_queue_minutes) + "\n")
                f.write("Mean between average_max_minutes and average_queue_minutes: " + str(
                    queueMin_MaxMinutesMean[-1]) + "\n")
                f.write("Standard Deviation between average_max_minutes and average_queue_minutes: " + str(
                    queueMin_MaxMinutesStanDev[-1]) + "\n")
                f.write("Percent Gain: " + str(percentGain[-1]) + "\n\n")

                # Write the print statements to the file
            f.write("Current WHERE CLAUSE: " + str(i) + " AND " + str(i + 60) + "\n")
            f.write("Total Number of Jobs: " +  str(total_jobs) + "\n")
            f.write("Current value of average max_minutes: " + str(average_max_minutes) + "\n")
            f.write("Current value of average queue_minutes: " + str(average_queue_minutes) + "\n")
            f.write("Mean between average_max_minutes and average_queue_minutes: " + str(
                queueMin_MaxMinutesMean[-1]) + "\n")
            f.write("Standard Deviation between average_max_minutes and average_queue_minutes: " + str(
                queueMin_MaxMinutesStanDev[-1]) + "\n")
            f.write("Percent Gain: " + str(percentGain[-1]) + "\n\n")


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
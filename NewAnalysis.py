from __future__ import print_function
import sys
import pandas as pd
import sqlalchemy as sa
import mysql.connector
import statistics
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
my_passwd = "password"  # Password for the user with the access mentioned on the line above
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

    
    # Max column values in the stampede2_jobq dataset 
    NORMALQUEUE_MAXTIME = 2880 # <- make smaller names, still be descriptive 
    MAX_MINUTESMAX = 10080
    BACKLOG_MINUTESMAX = 2562394
    BACKLOG_NUM_JOBSMAX = 1518
    QUEUE_MINUTESMAX = 90116

    # "Knobs" for the query adjustment
    loop_step_size = 10 # Knobs for for-loop range() function
    start_bound_ForLoop = 10
    end_bound_ForLoop = BACKLOG_NUM_JOBSMAX
    increase_factor = 60 # factor that is used to multiply some object by a factor to increase by, in this case by 5%
    standard_deviation_boundary = 50.0 # the percent acceptance rate that there may be a potential gain for reallocation
    num_jobs_boundary = 45 # the number of jobs that the user thinks is acceptable to think the standard deviation is a valid number
    # CONTINUED: this is important as to evaluate results quickly by looking at standard deviations that are considered significantly significant
    # as the conditions utilized are representative of jobs that could be affected in the mass

    STRTMAXMIN = 240
    ENDMAXMIN = 1500

    STRTBKLGMIN = 10000
    ENDBKLGMIN = BACKLOG_MINUTESMAX

    STRTBKLGNUMJOBS = 100
    ENDBKLGNUMJOBS = 500

    STRTQUEUEMIN = 100
    ENDQUEUEMIN = QUEUE_MINUTESMAX

    std_percentage_list = []


    with open('largeJobs_t1.txt', "w") as f:
        for i in range(start_bound_ForLoop, end_bound_ForLoop, loop_step_size):
            current_where_clause = " WHERE max_minutes BETWEEN " + str(STRTMAXMIN) + " AND " + str(ENDMAXMIN) + \
                                   " AND backlog_minutes BETWEEN " + str(STRTBKLGMIN) + " AND " + str(ENDBKLGMIN) + \
                                   " AND backlog_num_jobs BETWEEN " + str(STRTBKLGNUMJOBS) + " AND " + str(ENDBKLGNUMJOBS) + \
                                   " AND queue_minutes BETWEEN " + str(STRTQUEUEMIN) + " AND " + str(ENDQUEUEMIN) + ";"

            query = "SELECT COUNT(max_minutes) AS TotalJobs," \
                    "AVG(max_minutes) AS AverageMaxMinutesRequestedPerJob," \
                    "AVG(queue_minutes) AS AverageQueueMinutesEachJobExperienced," \
                    "AVG(backlog_minutes) AS AverageBacklogMinutes," \
                    "AVG(backlog_num_jobs) AS AverageNumberOfBacklogJobs," \
                    "stddev_samp(max_minutes) AS STDFORAverageMaxMinutesRequestedPerJob," \
                    "stddev_samp(queue_minutes) AS STDFORAverageQueueMinutesEachJobExperienced," \
                    "stddev_samp(backlog_minutes) AS STDFORAverageBacklogMinutes," \
                    "stddev_samp(backlog_num_jobs) AS STDFORAverageNumberOfBacklogJobs" \
                    " FROM HPC_Job_Database.stampede2_jobq" + current_where_clause

            print("\n", query)

            # str(i) + " AND " + str(i + loop_step_size)
            df_tmp = pd.read_sql(query, connection)

            #print("query", query)
            #print("\n",df)

            df = df_tmp.fillna(0) # Handles cases where the dataframe returns "NoneType" values because there is no data that is returned and as such replaces all those values with 0
            # Method not full proof, needs to be handled better to skip AND statements where that occurs

            total_jobs = float(df["TotalJobs"])

            if total_jobs == 0.0: # skips writing to file if there were no jobs in the query found, IE no statistics to be had
                print("\nNo jobs in this query, skipping...")
                STRTMAXMIN = STRTMAXMIN + 60
                ENDMAXMIN = STRTMAXMIN + 100
                STRTBKLGNUMJOBS = STRTBKLGNUMJOBS
                ENDBKLGNUMJOBS = ENDBKLGNUMJOBS
                STRTBKLGMIN = STRTBKLGMIN
                ENDBKLGMIN = ENDBKLGMIN
                continue

            average_max_minutes = float(df["AverageMaxMinutesRequestedPerJob"]) # Mean Max_Minutes Requested for the query
            average_queue_minutes = float(df["AverageQueueMinutesEachJobExperienced"]) # Mean Queue Minutes Experienced for the query
            average_backlog_minutes = float(df["AverageBacklogMinutes"]) # Mean Backlog Minutes Experienced for the query
            average_number_of_backlog_jobs = float(df["AverageNumberOfBacklogJobs"]) # Mean Backlog Number of Jobs Experienced for the query
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

            print("\nCurrent WHERE CLAUSE -> " + current_where_clause)

            print("Total Number of Jobs: ", total_jobs)

            print("Mean max_minutes:", average_max_minutes)
            print("Mean queue_minutes:", average_queue_minutes)
            print("Mean Backlog Number of Jobs:", average_number_of_backlog_jobs)
            print("Mean Backlog Minutes:", average_backlog_minutes)

            print("Mean max_minutes standard deviation:", std_for_average_max_minutes)
            print("Mean queue_minutes standard deviation", std_for_average_queue_minutes)
            print("Mean Backlog Number of Jobs standard deviation", std_for_average_backlog_minutes)
            print("Mean Backlog Minutes Requested standard deviation", std_for_average_backlog_minutes)


            # If the mean queue time experienced is within 50% of the mean requested max_minutes value, check for potential gain
            if average_queue_minutes > 0:
                #queue_percentage = (average_queue_minutes - average_max_minutes) / average_max_minutes # <- most likely not important, cut out
                #print("Queue percentage, IE it calculates how much the average queue time exceeds the average max time", queue_percentage)

                std_percentage = abs(std_for_average_queue_minutes / average_queue_minutes) * 100
                std_percentage_list.append(std_percentage)

                if std_percentage <= standard_deviation_boundary:
                    print("\nThe std_for_average_queue_minutes IS WITHIN +/- 50% of the average_queue_minutes -> " + str(std_percentage))
                    if total_jobs >= num_jobs_boundary:
                        print("\nStatisically significant std_for_average_queue_minutes found")
                        f.write("\n\nStatisically significant std_for_average_queue_minutes found. \nThe std_for_average_queue_minutes IS WITHIN +/- 50% of the average_queue_minutes -> " + str(std_percentage))
                    else:
                        f.write("\n\nThe std_for_average_queue_minutes IS WITHIN +/- 50% of the average_queue_minutes -> " + str(std_percentage) + " None significant")
                else:
                    print("\nThe std_for_average_queue_minutes is not within +/- 50% of the average_queue_minutes -> " + str(std_percentage))
                    f.write("\n\nThe std_for_average_queue_minutes IS NOT within +/- 50% of the average_queue_minutes -> " + str(std_percentage))

            # Write the print statements to the file
            f.write("\n\nWHERE CLAUSE -> \n" + current_where_clause + "\n")
            f.write("Total Number of Jobs: " + str(total_jobs) + "\n")

            f.write("\nMEANS DATA:\nMean max_minutes: " + str(average_max_minutes) + "\n")
            f.write("Mean queue_minutes: " + str(average_queue_minutes) + "\n")
            f.write("Mean Backlog Number of Jobs: " + str(average_number_of_backlog_jobs) + "\n")
            f.write("Mean Backlog Minutes: " + str(average_backlog_minutes) + "\n")

            f.write("\nSTANDARD DEVIATION DATA:\nMean max_minutes standard deviation: " + str(std_for_average_max_minutes) + "\n")
            f.write("Mean queue_minutes standard deviation: " + str(std_for_average_queue_minutes) + "\n")
            f.write("Mean Backlog Number of Jobs standard deviation: " + str(std_for_average_backlog_minutes) + "\n")
            f.write("Mean Backlog Minutes Requested standard deviation: " + str(std_for_average_backlog_minutes) + "\n--------------------------------------------")


            #f.write("\nQueue Percentage, IE it calculates how much the average queue time exceeds the average max time: " + str(queue_percentage) + "\n\n--------------------------------------------\n")

            # Increase values for the next iteration
            STRTMAXMIN = STRTMAXMIN + 60
            ENDMAXMIN = ENDMAXMIN + 100
            STRTBKLGNUMJOBS = STRTBKLGNUMJOBS
            ENDBKLGNUMJOBS = ENDBKLGNUMJOBS
            STRTBKLGMIN = STRTBKLGMIN
            ENDBKLGMIN = ENDBKLGMIN

            if ENDMAXMIN >= MAX_MINUTESMAX:
                print("\nRan out of requested max_minutes")
                break

        if std_percentage_list:
            mean_std_percentage = statistics.mean(std_percentage_list)
            f.write("\n\nMean Standard Deviation Percentage: " + str(mean_std_percentage))
            f.write("\nMaximum STD value: " + str(max(std_percentage_list)))
            f.write("\nMinimum STD value: " + str(min(std_percentage_list)))
            f.write("\nSTD values for run: \n")

            std_percentage_list_sorted = sorted(std_percentage_list)

            for value in std_percentage_list_sorted:
                f.write(f'{value}\n')
        else:
            mean_std_percentage = None  # handle for when the list is empty
            f.write("None was found in this trial")
        f.close

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
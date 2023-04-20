from __future__ import print_function
import sys
import pandas as pd
import sqlalchemy as sa
import mysql.connector
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

def query(connection):


    connection = connection

    # total_jobs_list = []
    # average_max_minutes_list = []
    # average_queue_minutes_list = []
    # average_backlog_minutes_list = []
    # average_number_of_backlog_jobs_list = []
    # std_for_average_max_minutes_list = []
    # std_for_average_queue_minutes_list = []
    # std_for_average_backlog_minutes_list = []
    # std_for_average_number_of_backlog_jobs_list = []
    std_percentage_list = []

    # Trial Analysis conditions
    standard_deviation_boundary = 50.0 # the percent acceptance rate that there may be a potential gain for reallocation
    num_jobs_boundary = 75 # the number of jobs that the user thinks is acceptable to think the standard deviation is a valid number
    # CONTINUED: this is important as to evaluate results quickly by looking at standard deviations that are considered significantly significant
    # as the conditions utilized are representative of jobs that could be affected in the mass

    MAX_MINUTES_MAX = 2880 # The actual maximum number is 10080; Run special tests for those jobs > 2880, there were 101 jobs
    BACKLOG_MINUTES_MAX = 2380000 # Actual maximum number is 2562394; Run special tests for thsoe jobs > 2380000, there are 566 jobs
    BACKLOG_NUM_JOBS_MAX = 1518
    QUEUE_MINUTES_MAX = 19000 # Actual maximum number is 90116; Run special tests for thsoe jobs > 19000, there are 268 jobs

    # Knob boundary conditions and corresponding 'ith' iteration values
    strt_max_min = 1 # Start Max_Minutes Boundary Cond.
    end_max_min = MAX_MINUTES_MAX # End Max_Minutes Boundary Cond.
    max_min_step = 120 # the incrementation value for max_min

    strt_bklg_min = 1
    end_bklg_min = BACKLOG_MINUTES_MAX
    backlog_min_step = 500

    strt_bklg_num_jobs = 1
    end_bklg_num_jobs = BACKLOG_NUM_JOBS_MAX
    backlog_jobs_step = 50

    str_queue_min = 1
    end_queue_min = QUEUE_MINUTES_MAX
    queue_min_step = 500

    good_data_count = 0 # counts how many of the queries in the dataset are "good" or produce a value that is non 0 

    start_time = time.time()

    with open('Stampede2_Normal_Queue_Bin_Sweep.txt', "a") as f:
        for i in range(strt_max_min, end_max_min, max_min_step):
            for j in range(strt_bklg_min, end_bklg_min, backlog_min_step):
                for k in range(strt_bklg_num_jobs, end_bklg_num_jobs, backlog_jobs_step):
                    for m in range(str_queue_min, end_queue_min, queue_min_step):
                        iter_start_time = time.time()
                        current_where_clause = " WHERE max_minutes BETWEEN " + str(i) + " AND " + str(i + max_min_step) + \
                                           " AND backlog_minutes BETWEEN " + str(j) + " AND " + str(j + backlog_min_step) + \
                                           " AND backlog_num_jobs BETWEEN " + str(k) + " AND " + str(k + backlog_jobs_step) + \
                                           " AND queue_minutes BETWEEN " + str(m) + " AND " + str(m + queue_min_step) + ";"

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

                        df_tmp_start_time = time.time()

                        df = pd.read_sql(query, connection).fillna(0)# Handles cases where the dataframe returns "NoneType" values because there is no data that is returned and as such replaces all those values with 0
                        # Method not full proof, needs to be handled better to skip AND statements where that occurs

                        total_jobs = float(df["TotalJobs"])

                        if total_jobs >= 30.0: # skips writing to file if there were no jobs in the query found, IE no statistics to be had for 0 cases as well as tiny jobs that only had 1 occurance
                            good_data_count += 1 
                            average_max_minutes = float(df["AverageMaxMinutesRequestedPerJob"]) # Mean Max_Minutes Requested for the query
                            average_queue_minutes = float(df["AverageQueueMinutesEachJobExperienced"]) # Mean Queue Minutes Experienced for the query
                            average_backlog_minutes = float(df["AverageBacklogMinutes"]) # Mean Backlog Minutes Experienced for the query
                            average_number_of_backlog_jobs = float(df["AverageNumberOfBacklogJobs"]) # Mean Backlog Number of Jobs Experienced for the query
                            std_for_average_max_minutes = float(df["STDFORAverageMaxMinutesRequestedPerJob"])
                            std_for_average_queue_minutes = float(df["STDFORAverageQueueMinutesEachJobExperienced"])
                            std_for_average_backlog_minutes = float(df["STDFORAverageBacklogMinutes"])
                            std_for_average_number_of_backlog_jobs = float(df["STDFORAverageNumberOfBacklogJobs"])
                            df_tmp_end_time = time.time()


                            # total_jobs_list.append(total_jobs)
                            # average_max_minutes_list.append(average_max_minutes)
                            # average_queue_minutes_list.append(average_queue_minutes)
                            # average_backlog_minutes_list.append(average_backlog_minutes)
                            # average_number_of_backlog_jobs_list.append(average_number_of_backlog_jobs)
                            # std_for_average_max_minutes_list.append(std_for_average_max_minutes)
                            # std_for_average_queue_minutes_list.append(std_for_average_queue_minutes)
                            # std_for_average_backlog_minutes_list.append(std_for_average_backlog_minutes)
                            # std_for_average_number_of_backlog_jobs_list.append(std_for_average_number_of_backlog_jobs)

                            # print("\nCurrent WHERE CLAUSE -> " + current_where_clause)
                            #
                            # print("Total Number of Jobs: ", total_jobs)
                            #
                            # print("Mean max_minutes:", average_max_minutes)
                            # print("Mean queue_minutes:", average_queue_minutes)
                            # print("Mean Backlog Number of Jobs:", average_number_of_backlog_jobs)
                            # print("Mean Backlog Minutes:", average_backlog_minutes)
                            #
                            # print("Mean max_minutes standard deviation:", std_for_average_max_minutes)
                            # print("Mean queue_minutes standard deviation", std_for_average_queue_minutes)
                            # print("Mean Backlog Number of Jobs standard deviation", std_for_average_backlog_minutes)
                            # print("Mean Backlog Minutes Requested standard deviation", std_for_average_backlog_minutes)


                            # If the mean queue time experienced is within 50% of the mean requested max_minutes value, check for potential gain
                            #if average_queue_minutes > 0:
                                #queue_percentage = (average_queue_minutes - average_max_minutes) / average_max_minutes # <- most likely not important, cut out
                                #print("Queue percentage, IE it calculates how much the average queue time exceeds the average max time", queue_percentage)

                            std_percentage = abs(std_for_average_queue_minutes / average_queue_minutes) * 100
                            std_percentage_list.append(std_percentage)

                            if std_percentage <= standard_deviation_boundary and total_jobs >= num_jobs_boundary:
                                print("\nStatistically significant std_for_average_queue_minutes found")
                                f.write("\n\nStatistically significant std_for_average_queue_minutes found. \nThe std_for_average_queue_minutes IS WITHIN +/- 50% of the average_queue_minutes -> " + str(std_percentage))
                            elif std_percentage <= standard_deviation_boundary and total_jobs < num_jobs_boundary:
                                #print("\nThe std_for_average_queue_minutes is within +/- 50% of the average_queue_minutes but not considered significant -> " + str(std_percentage))
                                f.write("\n\nThe std_for_average_queue_minutes is within +/- 50% of the average_queue_minutes but not considered significant -> " + str(std_percentage))
                            else:
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
                            f.write("Mean Backlog Minutes Requested standard deviation: " + str(std_for_average_number_of_backlog_jobs) + "\n--------------------------------------------")
                            iter_end_time = time.time()
                            print(f"Iteration {good_data_count}: {iter_end_time - iter_start_time:.5f} seconds")
                            print(f"Time between reading SQL and assigning value: {df_tmp_end_time - df_tmp_start_time:.5f} seconds")
                        else:
                            #print("\nNo jobs in this query, skipping...")
                            continue

                        #f.write("\nQueue Percentage, IE it calculates how much the average queue time exceeds the average max time: " + str(queue_percentage) + "\n\n--------------------------------------------\n")

                    # if std_percentage_list:
                    #     mean_std_percentage = statistics.mean(std_percentage_list)
                    #     f.write("\n\nMean Standard Deviation Percentage: " + str(mean_std_percentage))
                    #     f.write("\nMaximum STD value: " + str(max(std_percentage_list)))
                    #     f.write("\nMinimum STD value: " + str(min(std_percentage_list)))
                    #     f.write("\nSTD values for run: \n")
                    #
                    #     std_percentage_list_sorted = sorted(std_percentage_list)
                    #
                    #     for value in std_percentage_list_sorted:
                    #         f.write(f'{value}\n')
                    # else:
                    #     mean_std_percentage = None  # handle for when the list is empty
                    #     f.write("None was found in this trial")
        total_time = time.time() - start_time
        print(f"Total time taken: {total_time:.5f} seconds")
    f.write(f"\nNumber of \"good\" data returned, IE the number of queries in the dataset that returned a non 0 value: {good_data_count}")
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
        query(connection)
        #tempPlot(connection, mainConnection, tableName)
    #connection.close() # 'Engine' object has no attribute 'close'
    sys.exit(1)

if __name__ == '__main__':
    main()
# Smart Scheduling Data Injection 

# Description 
This smart scheduling project is designed to dynamically optimize job scheduling for jobs that are submmited to TACC-based
high-performance computer (HPC) systems through the TACC TAPIS framework to optimize queue time jobs experince in the TAPIS framework. 


# Script Overview

The ```HPCDataLoad.py``` program functions by reading in submitted job data generated by each HPC system, converts the pre-spliced data into a standardized format, which is inserted into a MySQL table 
by utilizing the ```mysql.connector``` Python library. The newly standardized data can be easily queried at the user's discretion by using SQL commands. 

To be able to run and fully utilize the ```HPCDataLoad.py``` script, this README assumes a decent level of understanding regarding programming in Python and 
MySQL Workbench. 

Below is an example of the pre-spliced data and its formatting: 
```commandline
JobID|User|Account|Start|End|Submit|Partition|Timelimit|JobName|State|NNodes|ReqCPUS|NodeList
4221134|tg840985|phy20012|2022-04-07T11:09:50|2022-04-08T07:50:34|2022-04-07T06:00:03|rtx|1-00:00:00|glidein|COMPLETED|1|16|c196-061
```
This "raw" data is in a text format, and as such is converted into a readable format that give each column and its respective data its proper data typing. 
Each of the data points column names is used to create the column names of the table where the data will be store. This submitted job data can be queried
to run statistical analysis to see how to best approach dynamically reallocating submitted jobs to the various HPC systems based on different factors such as node count or
the amount of requested CPUS.

# Instructions on How to Run Script 

This instruction is based on the presumption that both [MySQL Workbench](https://dev.mysql.com/downloads/workbench/) and an IDE that has the [Python Library](https://www.python.org/downloads/) and [MySQL Connector package](https://dev.mysql.com/doc/connector-python/en/connector-python-installation-binary.html) has 
been fully installed onto your host machine. Links have been provided to do as such. 

After downloading this Github repository onto your host machine, you should have access to both the datasets and the code 
scripts to run this program. 

Open up the ```HPCDataLoad.py``` file and look at lines 16-32:
```
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
partition_limit = 2880 # Default time limit for max job runtimes in TACC HPC systems - 2880 Minutes or 2 Days
# **************************************************************

```

Here are the runtime parameters predetermined to have a valid runtime environment for our script. 
The variables to note are ```my_user```, ```my_passwd```, ```my_database```. To set up these variables 
with their correct values, as they must be changed to your user specifications as they cannot be left as default, as such we need to create these values in our SQL workbench. 
In addition, based on your partition limit setting for your max job runtime, set ```partition_limit``` accordingly. 

## Creating the Database
Open up MySQL Workbench and click the plus sign by the "MySQL Connections". 

Set up the connection name, it may be whatever you would like it to be. Leave the hostname
variable set to "127.0.0.1", which is the IP address of your local machine. You may leave the username to 'root' and the password
as what you had previously set up, or change it to a user you had previously set up. To create a user,
continue reading below.

Go ahead and test the connection. After a successful connection, close the window and click on the connection instantiation. 

Click on the cylinder with the line through it and create a new database or 'schema'. 
Give it whatever name you would like and save the instance. 

To create a new user with full privileges, in the connection instance, click on
```Server```, followed by ```Users and Privileges``` and selecting ```Add Account```.
In the ```Admistrative Roles``` tab, select all the user privileges you would like for this
user to have and click on save. Go back and click on the house icon and right-click on the
connection instance and select ```Edit Connection```. Here, replace the username and password values
with the new user you created and save. Make sure to test the connection be sure everything is set up correctly.

Take the username, password, and database names that you previously created, open up ```HPCDataLoad.py``` and replace 
the default values previously mentioned above and replace them with your own. You can also change the path where the job data is 
stored on your local machine by changing the ```my_parent_dir``` to that directory path.

All the default variable names MUST be changed for this program to connect your database and dataset.

# Running the Script 
For running ```HPCDataLoad.py``` script, go to your terminal and run the following command:
```commandline
 $ python3 HPCDataLoad.py [Database Name] [HPC name]
```
Where the [Database Name] is the name of database you would like to access and [HPC name] is the name
of the HPC you would like to insert your data from. You must submit both arguments or an exit code will be prompted:
```commandline
Please enter the correct amount of command-line arguments (2) in their respective order:
python3 HPCDataLoad.py [Database Name] [Table Name]
```
For our example, let run the following, which will produce the following output:
```commandline
 $ python3 HPCDataLoad.py HPC_Job_Time_Data frontera
 
Successfully Connected to MySQL Server
Default database HPC_Job_Time_Data in use...

Successfully Connected to your MySQL Database: HPC_Job_Time_Data
Table frontera already exists

Start:  2022-10-13 21:34:43.610584

End:  2022-10-13 21:34:43.947078
Total record errors:  0
Total files skipped:  0
Total files read:  0
```

In the dataset, there are 6 HPC systems to choose from: Frontera, Stampede2, Stampede, Lonestar6, Longhorn, and Maverick. 
You may test any of these systems or any system that follow the TACC CRON job format. This script will insert the job accounting 
to the SQL table, tracking the total run time and record errors that occured. 

The script will either create the data table if it does not already exist and insert the data pertaining to each HPC
system into its respective table. In addition, to increase efficiency, a last read in file system is incorporated, which will
only allow the program to insert new job data if the data that is currently be read in is newer than the last data that was successfully committed to the table.

For any questions, feel free to message the admins of this Github repository: Costaki33 and richcar58.  
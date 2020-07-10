## Data Modeling with Cassandra

# Introduction:

Music Company Sparikfy wants to analyze the data for their app regarding user activity which is collected as log files. The log files are in JSON format. It is difficult to make insights out of the JSON format log files. So we decided to use non relational database to develop a datawarehouse. We have to have queries beforehand to make NoSQL database as a datawarehouse.


# Project Overview:

Data Modeling is made through Apache Cassandra while developing a ETL script which loads the data using Python scripting. ETL script dumps the data from csv file to a star schema based datawarehouse stored in NoSQL. THe database design is done prior to writing ETL script so we have right design before developing workflows.


# Provided Datasets:

The structure of directory of two files in the dataset: 

event_data/2018-11-08-events.csv 
event_data/2018-11-09-events.csv


# Project Steps:


Design the database structure to answer the business queries
All DDL and DML should be correctly structured in order to have no issues during ETL script
Make sure partioning by columns is done through appropriate way because Cassandra is a column oriented database
Script out ETL in Python to handle data and print query results

# Files :

Event_datafile_new.csv: Combined data of event files

Event_Data Folder: Separate event files

Project_1B_Project_Template.ipynb: Notebook for python scripting of ETL


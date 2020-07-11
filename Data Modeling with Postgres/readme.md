# Introduction

A startup called Sparkify want to analyze the data they have been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

The aim is to create a Postgres Database Schema and ETL pipeline to optimize queries for song play analysis.

# Project Description

In this project, we have to model data with Postgres and build and ETL pipeline using Python to perform optimized queries on song play analysis. On the database side, We have to define fact and dimension tables to use Star Schema and ETL pipeline should be created to transfer data from files located in two local directories into these tables in Postgres using Python and SQL.

## Schema for Song Play Analysis

## Fact Table

SongPlay stores the log data associated with song plays

## Dimension Tables

User in the application

Song in music database

Artist in music database

Time: timestamps of records in songplays broken down into specific units

## Project Design

Database Design is very beneficial for analytic queries, we can get the most information by joining tables and extract required information.

ETL Design is simplified just have to read json files and parse using pandas and to store the tables into specific columns with proper validation

Database Script

Writing "python create_tables.py" command in terminal, it is easier to create and recreate tables on each run

Jupyter Notebook

etl.ipynb, a Jupyter notebook is given for verifying each command and data as well and then using those statements and copying into etl.py and running it into terminal using "python etl.py" and then running test.ipynb to see whether data has been loaded in all the tables

Relevant Files Provided

test.ipnb displays the first few rows of each table to let you check your database

create_tables.py Create new database on every run , It will drop and create tables on every run

etl.ipynb read and processes a single file from song_data and log_data and loads into your tables in Jupyter notebook

etl.ipynb read and processes a single file from song_data and log_data and loads into your tables in ET

sql_queries.py containg all sql queries that will be used in this project

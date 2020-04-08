### Project Summary
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
You are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

### Database Schema
To complete the project, create a star schema optimized for queries on song play analysis. This includes the following tables.

## Staging Tables
staging_events

staging_songs

## Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables
users - users in the app user_id, first_name, last_name, gender, level

songs - songs in music database song_id, title, artist_id, year, duration

artists - artists in music database artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

## Template Files
create_table.py creates fact and dimension tables for the star schema in Redshift.

etl.py loads data from S3 into staging tables on Redshift and then processes that data into analytics tables on Redshift.

sql_queries.py defines you SQL statements, which will be imported into the two other files above.

README.md provides discussion on the process.

## Steps to Follow
Design schemas for fact and dimension tables.

Write a SQL CREATE statement for each of these tables in sql_queries.py.

Complete the logic in create_tables.py to connect to the database and create these tables.

Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist.

Launch a redshift cluster and create an IAM role that has read access to S3.

Add redshift database and IAM role info to dwh.cfg.

Test by running create_tables.py and checking the table schemas in the redshift database.

## ETL Pipiline
Implement the logic in etl.py to load data from S3 to staging tables on Redshift.

Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.

Test by running etl.py after running create_tables.py and running the analytic queries on the Redshift database to compare results with the expected results.

Delete the redshift cluster when finished.

## Exploratory Analytics
Number of rows in each table:

| Table            | Rows  |
|---               | --:   |
| staging_events   | 8056  |
| staging_songs    | 14896 |
| artists          | 10025 |
| songplays        | 320   |
| songs            | 14896 |
| time             | 320   |
| users            | 104   |

# ETL-with-Spark
Extract Transform Load with Spark, EMR cluster
# DATA LAKE PROJECT FOR SPARKIFY
-------------------------------------------------------------------------------
Sparkify is a music streaming company. It has grown the user base. Currently, the data resides in S3 in JSON format on user activity of the app. As a data engineer, my task is to build ETL pipeline that extracts data from S3, process with Spark and loads back into S3 as a set of dimentional tables. This allows the analytics team to continue finding insights in what songs their users are listening to.  

## DATASET
- Song Data: `s3://udacity-dend/song_data`
    - The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
    - `song_data/A/B/C/TRABCEI128F424C983.json`
    - `song_data/A/A/B/TRAABJL12903CDCF1A.json`
    - Raw data example:
    `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

- Log Data: `s3://udacity-dend/log_data`
    - The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.
    - `log_data/2018/11/2018-11-12-events.json`
    - `log_data/2018/11/2018-11-13-events.json`
    

## RESULTANT STAR SCHEMA FOR FACT AND DIMENSION TABLE
- Fact Table
    - songplays - records in log data associated with song plays i.e. records with page NextSong: 
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
- Dimension Tables
    - users - user_id, first_name, last_name, gender, level
    - songs - song_id, title, artist_id, year, duration
    - artists - artist_id, name, location, lattitude, longitude
    - time - start_time, hour, day, week, month, year, weekday
    
## REQUIREMENT
- Need to create IAM role with `programmatic` access and full access permission for S3.
- Need to create EMR cluster with 3 nodes: 1 master and 2 slaves.
    - EMR version: `emr-5.20.0` or later
    - Applications: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0
    - Instance type: m3.xlarge
    - EC2 key pair required to connect with the master node to submit the spark job or one can run using the `Jupyter Notebook` provided by EMR in AWS portal.
    
## STEP BY STEP PROJECT EXECUTION

- `dl.cfg`: Need to add access and secret key related to IAM user
- Need to create S3 bucket for the output_path
- `tl.py`has the code that loads data into cluster using spark and create fact and dimension tables based on the star schema mentioned above. 
- Run `tl.py`script in the cluster using ssh or copy paste the code in Jupyter Notebook provided by EMR itself.
- One can see the output in the output bucket that is created in S3. The data in S3 output folder will be stored as `parquet` format and are partitioned based on the given clauses.
    

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F
from pyspark.sql import types as T
import time

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark session in AWS
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
        
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extracts songs data, processes it and saves into parquet files 'songs' and 'artists' on S3 bucket
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data')
        #Use the below if you just want to test on smaller dataset
        #song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_output = os.path.join(output_data, "songs")
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_output, mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude")
    
    # write artists table to parquet files
    artists_output = os.path.join(output_data, "artists")
    artists_table.write.parquet(artists_output, mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Extracts log data, processes it and saves into parquet files 'time', 'users' and 'songplays' on S3 bucket
    """    
    
    # define the below to convert timestamp column later
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000.0),T.TimestampType())
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)/1000.0),T.DateType())
        
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')
        #Use the below if you just want to test on smaller dataset
        #log_data = os.path.join(input_data, 'log_data/2018/11/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays, 
    # define timestamp and datetime columns in the same step
    df = df[df['page']=="NextSong"].withColumn("timestamp", get_timestamp(df.ts)).withColumn("datetime", get_datetime(df.ts))

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level")
    
    # write users table to parquet files
    users_output = os.path.join(output_data, "users")
    users_table.write.parquet(users_output, mode="overwrite")

    # create timestamp column from original timestamp column
    # df = df.withColumn("timestamp", get_timestamp(df.ts)) # this has been already done above
    
    # create datetime column from original timestamp column
    # df = df.withColumn("datetime", get_datetime(df.ts)) # this has been already done above
    
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_date", 
                               "hour(timestamp) as hour",
                               "day(datetime) as day",
                               "weekofyear(datetime) as week",
                               "month(datetime) as month",
                               "year(datetime) as year",
                               "date_format(datetime, 'EEEE') as weekday"
                              )
    
    # write time table to parquet files partitioned by year and month
    time_output = os.path.join(output_data, "time")
    time_table.write.partitionBy("year", "month").parquet(time_output, mode="overwrite")

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data')    
        #Use the below if you just want to test on smaller dataset
        #song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), how='left')\
        .select(
            col("timestamp").alias('start_time'),
            col("userId").alias('user_id'),
            df.level,
            song_df.song_id,
            song_df.artist_id,
            col("sessionId").alias("session_id"),
            df.location,
            col("useragent").alias("user_agent"),
            year('datetime').alias('year'),
            month('datetime').alias('month')
        )

    # write songplays table to parquet files partitioned by year and month
    songplays_output = os.path.join(output_data, "songplays")
    songplays_table.write.partitionBy("year", "month").parquet(songplays_output, mode="overwrite")


def main():
    """
    Main function for this python file.
    Creates Spark session and process the data.
    Technically it runs create_spark_session(), process_song_data() and process_log_data()
    functions for input_data and output_data provided below
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://bartsdatalakepublic1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

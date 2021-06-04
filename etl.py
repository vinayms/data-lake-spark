import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_KEY']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_KEY']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This fucntion creates spark session with hadoop-aws configuration.
    Returns : spark object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads data from input_data and transforms to structured dimension table and writes to output_data location. 
    Dimension tables : songs_table and artists_table.
    
    Parameters : 
    spark (SparkSession) : Initiased spark object with configurations
    input_data (string)  : Input path string where the raw data to be read from.
    output_data (string) : The output file path where the transformed data should be written to.
        
    """
    # read song data file
    song_data_df = spark.read.json(input_data+"song_data/*/*/*")

    # extract columns to create songs table
    songs_table = song_data_df['song_id','title','artist_id','year','duration'].dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs.parquet'),'overwrite')
    print('songs_table write to output location completed ...')
    
    # extract columns to create artists table
    artists_table = song_data['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'].dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists.parquet'),'overwrite')
    print('artists_table write to output location completed ...')


def process_log_data(spark, input_data, output_data):
    """
    This function reads data from input_data location and process and transforms to structured dimension and fact table and writes to output_data location.
    Fact table      : songsplay_table 
    Dimension table : time_table
    Paramters : 
    spark (SparkSession) : Initiased spark object with configurations
    input_data (string)  : Input path string where the raw data to be read from.
    output_data (string) : The output file path where the transformed data should be written to.
    
    """
    # get filepath to log data file
    log_data = input_data+"/log_data/*/*/*.json"

    # read log data file
    log_data_df = spark.read.json(log_date)
    
    # filter by actions for song plays
    filter = log_data['page'] == 'NextSong'
    user_table = log_data['userId','firstName','lastName','gender','level'].where(filter).dropDuplicates(['userId'])
    
    # extract columns for users table    
    user_table = log_data_df['userId','firstName','lastName','gender','level'].dropDuplicates(['userId'])
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, 'user.parquet'), 'overwrite')
    print('user_table write to output location completed ...')

    get_timestamp = udf(lambda x:str(int(int(x)/1000)))
    get_datetime = udf(lambda x:str(datetime.fromtimestamp(int(x)/1000.0)))
    
    log_data_df = log_data_df.withColumn('timestamp',get_timestamp(log_data.ts))
    log_data_df = log_data_df.withColumn('datetime',get_datetime(log_data.ts))
    
    # extract columns to create time table
    time_table = log_data.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year')
    )
    time_table.dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    print('time_table write completed.. ')
    song_data_df = spark.read.json(input_data+"song_data/*/*/*")
    
    log_data_df = log_data_df.join(song_data_df, song_data_df.title == log_data_df.song)
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_data.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    )
    songplays_table.select(monotonically_increase_id().alias('songplay_id')).collect()
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print('songplays_table write completed.. ')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://vin-udacity-data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()

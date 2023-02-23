import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, DoubleType
from src.etl_internal import transform_movies_df, transform_streams_df
import datetime as dt


MOVIES_SCHEMA = StructType([ \
    StructField("title",StringType(),True), \
    StructField("duration_mins",IntegerType(),True), \
    StructField("original_language",StringType(),True), \
    StructField("size_mb", IntegerType(), True), \
  ])

STREAMS_SCHEMA = StructType([ \
    StructField("movie_title",StringType(),True), \
    StructField("user_email",StringType(),True), \
    StructField("size_mb",DoubleType(),True), \
    StructField("start_at", TimestampType(), True), \
    StructField("end_at", TimestampType(), True), \
  ])

FINAL_STREAMS_SCHEMA = StructType([ \
    StructField("movie_title",StringType(),True), \
    StructField("user_email",StringType(),True), \
    StructField("size_mb",DoubleType(),True), \
    StructField("start_at", TimestampType(), True), \
    StructField("end_at", TimestampType(), True), \
    StructField("watch_duration_mins", IntegerType(), True), \
    StructField("movie_duration_mins", IntegerType(), True), \
    StructField("perc_watch_stream", DoubleType(), True), \
  ])

# Get one spark session for the whole test session
@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.getOrCreate()

def test_transform_streams_df(spark_session) -> None:
    # arrange
    movies = [
      ("the great escape", 113, "Korean", 876),
      ("the third man", 129, "French", 1857),
    ]
    df_movies = spark_session.createDataFrame(data=movies, schema=MOVIES_SCHEMA)

    streams = [
      ("The Great Escape", "email@hills.info", 457.3093347690311, dt.datetime(2021, 12, 12, 10, 31, 4), dt.datetime(2021, 12, 12, 14, 41, 33)),
      ("Full Metal Jacket", "email@@murray.co", 613.4245936603365, dt.datetime(2021, 12, 6, 19, 30, 19), dt.datetime(2021, 12, 7, 15, 44, 38)),
    ]
    df_streams = spark_session.createDataFrame(data=streams, schema=STREAMS_SCHEMA)

    expected = [
      ("the great escape", "email@hills.info", 457.3093347690311, dt.datetime(2021, 12, 12, 10, 31, 4), dt.datetime(2021, 12, 12, 14, 41, 33), 250, 113, 2.2123893805309733),
    ]
    df_expected = spark_session.createDataFrame(data=expected, schema=FINAL_STREAMS_SCHEMA)

    # act
    df_final = transform_streams_df(df_streams, df_movies)

    # assert
    assert sorted(df_expected.collect()) == sorted(df_final.collect())

def test_transform_movies_df(spark_session) -> None:
    # arrange
    movies = [
      ("The Great Escape", 113, "Korean", 876),
      ("The Third Man", 129, "French", 1857),
    ]
    df_movies = spark_session.createDataFrame(data=movies, schema=MOVIES_SCHEMA)

    expected = [
      ("the great escape", 113, "Korean", 876),
      ("the third man", 129, "French", 1857),
    ]
    df_expected = spark_session.createDataFrame(data=expected, schema=MOVIES_SCHEMA)

    # act
    df_final = transform_movies_df(df_movies)

    # assert
    assert sorted(df_expected.collect()) == sorted(df_final.collect())
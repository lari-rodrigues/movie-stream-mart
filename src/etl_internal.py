from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower
from pyspark.sql.types import IntegerType, LongType
import logging

from src.helper.spark_session_builder import SparkSessionBuilder

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_csv(name: str, spark: SparkSession) -> DataFrame:
    logger.info(f"Starting reading {name} csv")
    df = spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(f"{name}.csv")
    return df


def transform_streams_df(df_streams: DataFrame, df_movies: DataFrame) -> DataFrame:
    logger.info("Starting to transform streams data")
    diff_secs_col = col("end_at").cast(LongType()) - col("start_at").cast(LongType())
    df_streams = (df_streams.withColumn("watch_duration_mins", (diff_secs_col / 60).cast(IntegerType()))
                  .withColumn("movie_title", lower(col("movie_title"))))

    df_movies_tmp = (df_movies[[
        "title",
        "duration_mins"
    ]].withColumnRenamed("title", "movie_title")
      .withColumnRenamed("duration_mins", "movie_duration_mins"))

    df_streams = df_streams.join(df_movies_tmp, on="movie_title")
    df_streams = df_streams.withColumn("perc_watch_stream", col("watch_duration_mins") / col("movie_duration_mins"))

    return df_streams


def transform_movies_df(df_movies: DataFrame) -> DataFrame:
    logger.info("Starting to transform movies data")
    df_movies = df_movies.withColumn("title", lower(col("title")))

    return df_movies


def persist_table(df: DataFrame, table_name: str):
    logger.info(f"Persisting table {table_name}")
    (df.write
     .format("delta")
     .mode("overwrite")
     .saveAsTable(table_name)
     )


def process(movies_path: str, streams_path: str):
    spark = SparkSessionBuilder().build()

    # extract
    df_movies = read_csv(movies_path, spark)
    df_streams = read_csv(streams_path, spark)

    # transform
    df_movies = transform_movies_df(df_movies)
    df_streams = transform_streams_df(df_streams, df_movies)

    # load
    persist_table(df_movies, "movies")
    persist_table(df_streams, "streams")


if __name__ == '__main__':
    prefix_path = "data/internal"
    process(movies_path=f"{prefix_path}/movies", streams_path=f"{prefix_path}/streams")

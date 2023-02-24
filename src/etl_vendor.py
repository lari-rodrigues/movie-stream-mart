from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, explode, concat_ws
from pyspark.sql.types import StructType
from delta import DeltaTable
import logging

from src.helper.spark_session_builder import SparkSessionBuilder

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_json(path: str, spark: SparkSession) -> DataFrame:
    logger.info(f"Starting reading {path} json")
    df = spark.read.option("multiline", "true").json(f"{path}.json")
    return df


def flatten(schema, prefix: str = ""):
    logger.info(f"Starting flattening json with schema {schema}")

    def field_items(field):
        name = f'{prefix}.{field.name}' if prefix else field.name
        if type(field.dataType) == StructType:
            return flatten(field.dataType, name)
        else:
            return [col(name)]

    return [item for field in schema.fields for item in field_items(field)]


def read_and_flat_json_df(path: str, spark: SparkSession) -> DataFrame:
    df = read_json(path, spark)
    flattened = flatten(df.schema)
    return df.select(*flattened)


def transform_books(df_books: DataFrame) -> DataFrame:
    df_books = (df_books
                .withColumn("name", lower(col("name")))
                .withColumn("author", lower(col("author")))
                .withColumn("publisher", lower(col("publisher"))))
    return df_books


def transform_reviews(df_reviews: DataFrame) -> DataFrame:
    df_reviews = (df_reviews.withColumnRenamed("created", "created_at")
                  .withColumnRenamed("updated", "updated_at"))
    df_reviews = (df_reviews
                  .withColumn("tmp", explode("movies"))
                  .select("created_at", "tmp.title", "updated_at", "rate", "label", "text", "books")
                  .filter(col("title") != "end")
                  .withColumnRenamed("title", "movie_title"))

    df_reviews = (df_reviews
                  .withColumn("tmp", explode("books"))
                  .select("created_at", "tmp.metadata.title", "movie_title", "updated_at", "rate", "label", "text")
                  .withColumnRenamed("title", "book_name"))

    df_reviews = (df_reviews
                  .withColumn("movie_title", lower(col("movie_title")))
                  .withColumn("book_name", lower(col("book_name"))))
    return df_reviews


def transform_authors(df_authors: DataFrame) -> DataFrame:
    df_authors = (df_authors
                  .withColumn("nationalities", concat_ws(",", col("nationalities.slug")))
                  .withColumn("name", lower(col("name"))))
    return df_authors


def persist_books(df_books: DataFrame, spark: SparkSession):
    logger.info("Persisting books")
    books_pk = ["name", "author"]
    (DeltaTable.forName(spark, "books").merge(df_books.alias('df'), 
                    ' and '.join([f'books.{k} = df.{k}' for k in books_pk]))
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute())


def persist_reviews(df_reviews: DataFrame, spark: SparkSession):
    logger.info("Persisting reviews")
    authors_pk = ["movie_title", "created_at"]
    (DeltaTable.forName(spark, "reviews").merge(df_reviews.alias('df'), 
                    ' and '.join([f'reviews.{k} = df.{k}' for k in authors_pk]))
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute())

def persist_authors(df_authors: DataFrame, spark: SparkSession):
    logger.info("Persisting authors")
    authors_pk = ["name"]
    (DeltaTable.forName(spark, "authors").merge(df_authors.alias('df'), 
                    ' and '.join([f'authors.{k} = df.{k}' for k in authors_pk]))
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute())


def process(authors_path: str, books_path: str, reviews_path: str):
    spark = SparkSessionBuilder().build()

    # extract
    df_authors = read_and_flat_json_df(authors_path, spark)
    df_books = read_and_flat_json_df(books_path, spark)
    df_reviews = read_and_flat_json_df(reviews_path, spark)

    # transform
    df_books = transform_books(df_books)
    df_reviews = transform_reviews(df_reviews)
    df_authors = transform_authors(df_authors)

    # load
    persist_books(df_books, spark)
    persist_reviews(df_reviews, spark)
    persist_authors(df_authors, spark)


if __name__ == '__main__':
    prefix_path = "data/vendor"
    process(authors_path=f"{prefix_path}/authors", 
            books_path=f"{prefix_path}/books", 
            reviews_path=f"{prefix_path}/reviews")

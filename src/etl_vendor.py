from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, explode, concat_ws
from pyspark.sql.types import StructType
from delta import DeltaTable
import logging

from src.helper.spark_session_builder import SparkSessionBuilder

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_json(name: str, spark: SparkSession) -> DataFrame:
    logger.info(f"Starting reading {name} json")
    df = spark.read.json(f"data/vendor/{name}.json")
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


def read_and_flat_json_df(name: str, spark: SparkSession) -> DataFrame:
    df = read_json(name, spark)
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
    delta_books = DeltaTable.forName(spark, "books")
    (delta_books.alias('books')
     .merge(df_books.alias('updates'),
            'books.name = updates.name and books.author = updates.author'
            ).whenMatchedUpdate(set={
        "pages": "updates.pages",
        "publisher": "updates.publisher",
    }
    ).whenNotMatchedInsert(values={
        "author": "updates.author",
        "name": "updates.name",
        "pages": "updates.pages",
        "publisher": "updates.publisher",
    }
    ).execute())


def persist_reviews(df_reviews: DataFrame, spark: SparkSession):
    logger.info("Persisting reviews")
    delta_reviews = DeltaTable.forName(spark, "reviews")
    (delta_reviews.alias('reviews')
     .merge(df_reviews.alias('updates'),
            'reviews.movie_title = updates.movie_title and reviews.created_at = updates.created_at'
            ).whenMatchedUpdate(set={
        "text": "updates.text",
        "label": "updates.label",
        "rate": "updates.rate",
        "updated_at": "updates.updated_at",
        "book_name": "updates.book_name",
    }
    ).whenNotMatchedInsert(values={
        "movie_title": "updates.movie_title",
        "created_at": "updates.created_at",
        "text": "updates.text",
        "label": "updates.label",
        "rate": "updates.rate",
        "updated_at": "updates.updated_at",
        "book_name": "updates.book_name",
    }
    ).execute())


def persist_authors(df_authors: DataFrame, spark: SparkSession):
    logger.info("Persisting authors")
    delta_authors = DeltaTable.forName(spark, "authors")
    (delta_authors.alias('authors')
     .merge(df_authors.alias('updates'),
            'authors.name = updates.name'
            ).whenMatchedUpdate(set={
        "birth_date": "updates.birth_date",
        "died_at": "updates.died_at",
        "nationalities": "updates.nationalities",
    }
    ).whenNotMatchedInsert(values={
        "name": "updates.name",
        "birth_date": "updates.birth_date",
        "died_at": "updates.died_at",
        "nationalities": "updates.nationalities",
    }
    ).execute())


def main():
    spark = SparkSessionBuilder().build()

    # extract
    df_authors = read_and_flat_json_df("authors", spark)
    df_books = read_and_flat_json_df("books", spark)
    df_reviews = read_and_flat_json_df("reviews", spark)

    # transform
    df_books = transform_books(df_books)
    df_reviews = transform_reviews(df_reviews)
    df_authors = transform_authors(df_authors)

    # load
    persist_books(df_books, spark)
    persist_reviews(df_reviews, spark)
    persist_authors(df_authors, spark)


if __name__ == '__main__':
    main()

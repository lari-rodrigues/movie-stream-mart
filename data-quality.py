from pyspark.sql import DataFrame
import logging

from src.helper.spark_session_builder import SparkSessionBuilder

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def check_column_not_null(df: DataFrame, column_name: str, table_name: str):
    df_temp = df[df[column_name].isNull()]
    if df_temp.count() > 0:
        logger.warning(f"{df_temp.count()} records null at {table_name}.{column_name}")


def check_table_not_empty(df: DataFrame, table_name: str):
    if df.count() == 0:
        logger.warning(f"{table_name} is empty")


def check_authors_data_quality(df_authors: DataFrame, table_name: str):
    check_column_not_null(df_authors, "name", table_name)
    check_column_not_null(df_authors, "birth_date", table_name)
    check_column_not_null(df_authors, "nationalities", table_name)


def check_books_data_quality(df_books: DataFrame, df_authors: DataFrame, table_name: str):
    check_column_not_null(df_books, "name", table_name)
    check_column_not_null(df_books, "author", table_name)
    check_column_not_null(df_books, "pages", table_name)
    check_column_not_null(df_books, "publisher", table_name)

    df_tmp = df_books.join(
        df_authors,
        df_books.author == df_authors.name,
        'left_anti',
    )
    if df_tmp.count() > 0:
        logger.warning(f"{df_tmp.count()} found at books that not exists equivalent at authors")
        logger.warning(df_tmp.select("author").distinct().show())


def check_reviews_data_quality(df_reviews: DataFrame, df_books: DataFrame, df_movies: DataFrame, table_name: str):
    check_column_not_null(df_reviews, "movie_title", table_name)
    check_column_not_null(df_reviews, "book_name", table_name)
    check_column_not_null(df_reviews, "created_at", table_name)
    check_column_not_null(df_reviews, "updated_at", table_name)
    check_column_not_null(df_reviews, "label", table_name)
    check_column_not_null(df_reviews, "rate", table_name)

    df_tmp = df_reviews.join(
        df_books,
        df_books.name == df_reviews.book_name,
        'left_anti',
    )
    if df_tmp.count() > 0:
        logger.warning(f"{df_tmp.count()} found at reviews that not exists equivalent at books")
        logger.warning(df_tmp.select("book_name").distinct().show())

    df_tmp = df_reviews.join(
        df_movies,
        df_movies.title == df_reviews.movie_title,
        'left_anti',
    )
    if df_tmp.count() > 0:
        logger.warning(f"{df_tmp.count()} found at reviews that not exists equivalent at movies")
        logger.warning("Probably these movies reviewed  are not at the streaming catalog. Opportunity to expand catalog")
        logger.warning(df_tmp.select("movie_title").distinct().show())


def main():
    spark = SparkSessionBuilder().build()

    # Reading tables
    df_books = spark.sql(f"select * from books")
    df_authors = spark.sql(f"select * from authors")
    df_reviews = spark.sql(f"select * from reviews")
    df_movies = spark.sql(f"select * from movies")

    # Checking Data Quality
    check_books_data_quality(df_books, df_authors, "books")
    check_authors_data_quality(df_authors, "authors")
    check_reviews_data_quality(df_reviews, df_books, df_movies, "reviews")


if __name__ == '__main__':
    main()

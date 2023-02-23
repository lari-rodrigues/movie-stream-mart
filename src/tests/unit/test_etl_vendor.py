import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, ArrayType, LongType
from src.etl_vendor import transform_authors, transform_books, transform_reviews
import datetime as dt


AUTHORS_SCHEMA = StructType([ \
    StructField("birth_date",StringType(),True), \
    StructField("died_at",StringType(),True), \
    StructField("name",StringType(),True), \
    StructField("nationalities", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("label", StringType(), True),
        StructField("slug", StringType(), True),
      ])), True), \
  ])

BOOKS_SCHEMA = StructType([ \
    StructField("author",StringType(),True), \
    StructField("name",StringType(),True), \
    StructField("pages",StringType(),True), \
    StructField("publisher", StringType(), True), \
  ])
  
REVIEWS_SCHEMA = StructType([ \
    StructField("books", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("metadata", StructType([
          StructField("pages",StringType(),True),
          StructField("title",StringType(),True),
          ]), True),
      ])), True), \
    StructField("text",StringType(),True), \
    StructField("created",StringType(),True), \
    StructField("movies", ArrayType(StructType([
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
      ])), True), \
    StructField("label",StringType(),True), \
    StructField("rate",StringType(),True), \
    StructField("updated",StringType(),True), \
  ])

FINAL_AUTHORS_SCHEMA = StructType([ \
    StructField("birth_date",StringType(),True), \
    StructField("died_at",StringType(),True), \
    StructField("name",StringType(),True), \
    StructField("nationalities",StringType(),True), \
  ])

FINAL_REVIEWS_SCHEMA = StructType([ \
    StructField("created_at",StringType(),True), \
    StructField("book_name",StringType(),True), \
    StructField("movie_title",StringType(),True), \
    StructField("updated_at",StringType(),True), \
    StructField("rate",StringType(),True), \
    StructField("label",StringType(),True), \
    StructField("text",StringType(),True), \
  ])

# Get one spark session for the whole test session
@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.getOrCreate()

def test_transform_authors_df(spark_session) -> None:
    # arrange
    authors = [
      (dt.datetime(1956, 5, 7), None, "Josh Johnston", [
        {"id": None, "label": "Guianese (French)", "slug": "guianese-french"},
        {"id": None, "label": " ", "slug": None}
      ]),
    ]
    df_authors = spark_session.createDataFrame(data=authors, schema=AUTHORS_SCHEMA)

    expected = [
      (dt.datetime(1956, 5, 7), None, "josh johnston", "guianese-french"),
    ]
    df_expected = spark_session.createDataFrame(data=expected, schema=FINAL_AUTHORS_SCHEMA)

    # act
    df_final = transform_authors(df_authors)

    # assert
    assert sorted(df_expected.collect()) == sorted(df_final.collect())


def test_transform_books_df(spark_session) -> None:
    # arrange
    books = [
      ("Ms. Dorsey Grant", "An Evil Cradling", 6302, "Harcourt Assessment"),
    ]
    df_books = spark_session.createDataFrame(data=books, schema=BOOKS_SCHEMA)

    expected = [
      ("ms. dorsey grant", "an evil cradling", 6302, "harcourt assessment"),
    ]
    df_expected = spark_session.createDataFrame(data=expected, schema=BOOKS_SCHEMA)

    # act
    df_final = transform_books(df_books)

    # assert
    assert sorted(df_expected.collect()) == sorted(df_final.collect())

def test_transform_reviews_df(spark_session) -> None:
    # arrange
    reviews = [
      (
        [
          {"id": None, "metadata": {"pages": "unknown", "title": "A PASSAGE TO INDIA"}},
        ], "Solitudo magnam vulpes. Aequitas vitium deputo.", dt.datetime(2021, 12, 2, 6, 11, 35), 
        [
          {"id": 0, "title": "BEAUTY AND THE BEAST"},
          {"id": 0, "title": "end"},
        ], "ONE", 1, dt.datetime(2021, 12, 17, 11, 49, 40)
      )
    ]
    df_reviews = spark_session.createDataFrame(data=reviews, schema=REVIEWS_SCHEMA)

    expected = [
      (dt.datetime(2021, 12, 2, 6, 11, 35), "a passage to india", "beauty and the beast", dt.datetime(2021, 12, 17, 11, 49, 40), "1", "ONE", "Solitudo magnam vulpes. Aequitas vitium deputo.")
    ]
    df_expected = spark_session.createDataFrame(data=expected, schema=FINAL_REVIEWS_SCHEMA)

    # act
    df_final = transform_reviews(df_reviews)

    # assert
    assert sorted(df_expected.collect()) == sorted(df_final.collect())


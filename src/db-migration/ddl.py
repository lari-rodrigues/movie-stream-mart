from delta import DeltaTable
from helper.spark_session_builder import SparkSessionBuilder


def main():
    spark = SparkSessionBuilder().build()

    (DeltaTable.createOrReplace(spark)
     .tableName("movies")
     .addColumn("title", "STRING")
     .addColumn("duration_mins", "INT")
     .addColumn("original_language", "STRING")
     .addColumn("size_mb", "INT")
     .property("description", "table with movies")
     .execute())

    (DeltaTable.createOrReplace(spark)
     .tableName("streams")
     .addColumn("movie_title", "STRING")
     .addColumn("user_email", "STRING")
     .addColumn("size_mb", "DOUBLE")
     .addColumn("start_at", "TIMESTAMP")
     .addColumn("end_at", "TIMESTAMP")
     .addColumn("watch_duration_mins", "INTEGER")
     .addColumn("movie_duration_mins", "INTEGER")
     .addColumn("perc_watch_stream", "DOUBLE")
     .property("description", "table with streams")
     .execute())

    (DeltaTable.createOrReplace(spark)
     .tableName("books")
     .addColumn("author", "STRING")
     .addColumn("name", "STRING")
     .addColumn("pages", "BIGINT")
     .addColumn("publisher", "STRING")
     .property("description", "table with books")
     .execute())

    (DeltaTable.createOrReplace(spark)
     .tableName("reviews")
     .addColumn("text", "STRING")
     .addColumn("movie_title", "STRING")
     .addColumn("book_name", "STRING")
     .addColumn("created_at", "TIMESTAMP")
     .addColumn("updated_at", "TIMESTAMP")
     .addColumn("label", "STRING")
     .addColumn("rate", "INTEGER")
     .property("description", "table with reviews")
     .execute())

    (DeltaTable.createOrReplace(spark)
     .tableName("authors")
     .addColumn("name", "STRING")
     .addColumn("birth_date", "TIMESTAMP")
     .addColumn("died_at", "TIMESTAMP")
     .addColumn("nationalities", "STRING")
     .property("description", "table with authors")
     .execute())


if __name__ == '__main__':
    main()

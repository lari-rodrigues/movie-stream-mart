from src.etl_vendor import process


def test_process(spark_session) -> None:
    # arrange
    prefix_path = "src/tests/integrated/mock_files/vendor"
   
    # act
    process(authors_path=f"{prefix_path}/authors", books_path=f"{prefix_path}/books", reviews_path=f"{prefix_path}/reviews")

    # assert
    final_df = spark_session.table("authors")
    assert final_df.count() == 2
    assert final_df.columns == ['name', 'birth_date', 'died_at', 'nationalities']

    final_df = spark_session.table("books")
    assert final_df.count() == 3
    assert final_df.columns == ['author', 'name', 'pages', 'publisher']


    final_df = spark_session.table("reviews")
    assert final_df.count() == 3
    assert final_df.columns == ['text', 'movie_title', 'book_name', 'created_at', 'updated_at', 'label', 'rate']


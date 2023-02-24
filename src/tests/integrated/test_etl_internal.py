from src.etl_internal import process


def test_process(spark_session) -> None:
    # arrange
    prefix_path = "src/tests/integrated/mock_files/internal"
   
    # act
    process(movies_path=f"{prefix_path}/movies", streams_path=f"{prefix_path}/streams")

    # assert
    final_df = spark_session.table("movies")
    assert final_df.count() == 2
    assert final_df.columns == ['title', 'duration_mins', 'original_language', 'size_mb']

    final_df = spark_session.table("streams")
    assert final_df.count() == 1
    assert final_df.columns == ['movie_title', 'user_email', 'size_mb', 'start_at', 'end_at', 'watch_duration_mins', 'movie_duration_mins', 'perc_watch_stream']



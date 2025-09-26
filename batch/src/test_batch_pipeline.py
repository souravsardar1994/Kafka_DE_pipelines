import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from batch_pipeline import remove_extra_spaces


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("testing").getOrCreate()
    yield spark


def test_single_space(spark_fixture):
    sample_data = [{"name": "John    D.", "age": 30},
                   {"name": "Alice   G.", "age": 25},
                   {"name": "Bob  T.", "age": 35},
                   {"name": "Eve   A.", "age": 28}]

    original_df = spark_fixture.createDataFrame(sample_data)
    transformed_df = remove_extra_spaces(original_df, "name")

    expected_data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 28}
    ]

    expected_df = spark_fixture.createDataFrame(expected_data)

    assertDataFrameEqual(transformed_df, expected_df)

import pytest
from chispa.dataframe_comparer import *
import pyspark.sql.functions as F
from app.utils import filter_column_by_list, order_dataframe, extract_pattern, get_top_performers
from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .master("local")
  .appName("chispa")
  .getOrCreate())

def test_filter_column_by_list():
    """Test the extraction of top sold products per department."""
    source_data = [
        (1, "IT", 54, 41),
        (2, "Marketing", 20, 1),
        (3, "Marketing", 15, 2),
        (4, "Games", 5, 1),
        (5, "Finance", 25, 2),
    ]

    source_df = spark.createDataFrame(source_data, ["id","area","calls_made","calls_successful"])

    actual_data = filter_column_by_list(source_df, "area", ["Marketing"])
    
    expected_data = [
        (2, "Marketing", 20, 1),
        (3, "Marketing", 15, 2),
    ]
    
    # This is the function to test
    filtered_data = filter_column_by_list(actual_data, "area", ["Marketing"])
    
    expected_df = spark.createDataFrame(expected_data, ["id","area","calls_made","calls_successful"])
    
    # Validate the result
    assert_df_equality(filtered_data, expected_df, ignore_row_order=True)


def test_order_dataframe():
    """Test ordering DataFrame in descending order."""
    source_data = [
        (1, "IT", 54, 41),
        (2, "Marketing", 20, 1),
        (3, "Marketing", 15, 2),
        (4, "Games", 5, 1),
        (5, "Finance", 25, 2),
    ]
    
    source_df = spark.createDataFrame(source_data, ["id", "area", "calls_made", "calls_successful"])
    
    # Call the function to test
    ordered_df = order_dataframe(source_df, "calls_made", ascending=False)

    expected_data = [
        (1, "IT", 54, 41),
        (2, "Marketing", 20, 1),
        (5, "Finance", 25, 2),
        (3, "Marketing", 15, 2),
        (4, "Games", 5, 1),
    ]
    
    expected_df = spark.createDataFrame(expected_data, ["id", "area", "calls_made", "calls_successful"])
    
    # Validate the result
    assert_df_equality(ordered_df, expected_df, ignore_row_order=True)


def test_extract_pattern():
    """Test the extraction of a pattern (postcode) from a DataFrame."""
    
    # Sample data with an address column
    source_data = [
        (1, "Thijmenweg 38, 7801 OC, Grijpskerk"),
        (2, "Lindehof 5, 4133 HB, Nederhemert"),
        (3, "4431 BT, Balinge"),
    ]
    
    # Create a DataFrame
    source_df = spark.createDataFrame(source_data, ["id", "address"])

    # Regex pattern to extract Dutch postcodes
    regex = r'(\d{4}\s?[A-Z]{2})'

    # Apply the extract_pattern function to extract postcodes
    actual_df = extract_pattern(source_df, 'postcode', 'address', regex)

    # Expected output DataFrame with postcodes
    expected_data = [
        (1, "Thijmenweg 38, 7801 OC, Grijpskerk", "7801 OC"),
        (2, "Lindehof 5, 4133 HB, Nederhemert", "4133 HB"),
        (3, "4431 BT, Balinge", "4431 BT"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "address", "postcode"])

    # Validate the result
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_extract_pattern():
    """Test the extraction of a pattern (postcode) from a DataFrame with names and addresses."""
    
    # Sample data with name, address, and sales amount columns
    source_data = [
        (1, "Sep Cant-Vandenbergh", "2588 VD, Kropswolde", 57751.6),
        (2, "Evie Godfrey van Alemannië-Smits", "1808 KR, Benningbroek", 69087.12),
        (3, "Vincent Mathurin", "Lindehof 5, 4133 HB, Nederhemert", 44933.21),
        (4, "Jolie Tillmanno", "4273 SW, Wirdum Gn", 44052.15),
        (5, "Faas Haring", "4431 BT, Balinge", 43985.41),
    ]
    
    # Create a DataFrame
    source_df = spark.createDataFrame(source_data, ["id", "name", "address", "sales_amount"])

    # Regex pattern to extract postcodes
    regex = r'(\d{4}\s?[A-Z]{2})'

    # Apply the extract_pattern function to extract postcodes
    actual_df = extract_pattern(source_df, 'postcode', 'address', regex)

    # Expected output DataFrame with postcodes extracted
    expected_data = [
        (1, "Sep Cant-Vandenbergh", "2588 VD, Kropswolde", 57751.6, "2588 VD"),
        (2, "Evie Godfrey van Alemannië-Smits", "1808 KR, Benningbroek", 69087.12, "1808 KR"),
        (3, "Vincent Mathurin", "Lindehof 5, 4133 HB, Nederhemert", 44933.21, "4133 HB"),
        (4, "Jolie Tillmanno", "4273 SW, Wirdum Gn", 44052.15, "4273 SW"),
        (5, "Faas Haring", "4431 BT, Balinge", 43985.41, "4431 BT"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "name", "address", "sales_amount", "postcode"])

    # Validate the result
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)

def test_get_top_performers():
    """Test retrieving the top N performers by grouping and ordering by specified columns."""

    # Sample data: id, department/area, sales_amount
    source_data = [
        (1, "Marketing", 57751.6),
        (2, "Marketing", 69087.12),
        (3, "Finance", 44933.21),
        (4, "Finance", 44052.15),
        (5, "IT", 63660.33),
        (6, "IT", 43985.41),
        (7, "Games", 33000.0),
        (8, "Games", 35000.0),
        (9, "Games", 45000.0),
    ]
    
    # Create DataFrame from the sample data
    source_df = spark.createDataFrame(source_data, ["id", "area", "sales_amount"])

    # Apply the get_top_performers function to get the top 2 performers per area
    actual_df = get_top_performers(source_df, "area", "sales_amount", 2)

    # Expected output data: top 2 performers per area ordered by sales_amount
    expected_data = [
        (2, "Marketing", 69087.12, 1),
        (1, "Marketing", 57751.6, 2),
        (5, "IT", 63660.33, 1),
        (6, "IT", 43985.41, 2),
        (9, "Games", 45000.0, 1),
        (8, "Games", 35000.0, 2),
        (3, "Finance", 44933.21, 1),
        (4, "Finance", 44052.15, 2),
    ]
    
    # Expected DataFrame based on the above expected data
    expected_df = spark.createDataFrame(expected_data, ["id", "area", "sales_amount", "rank"])

    # Validate the result using chispa's assert_df_equality
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_nullable=True)


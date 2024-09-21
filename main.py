from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
import sys

spark = SparkSession \
    .builder \
    .appName("abnamro") \
    .getOrCreate()

# Configure logging
def setup_logging():
    log_file = 'application.log'
    max_file_size = 5 * 1024 * 1024  # 5 MB
    backup_count = 3  # Keep 3 backup files

    handler = RotatingFileHandler(log_file, maxBytes=max_file_size, backupCount=backup_count)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)

def load_csv_in_spark(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Uses a spark session to read a csv file into a spark dataframe

    :param spark: The active spark session
    :param file_path: The path to the csv file to be loaded

    :returns: The file's contents in a spark DataFrame
    """
    if not Path(file_path).exists():
        logging.info("File does not exist.")
        raise ValueError("The specified file does not exist.")
    dataframe = (spark.read.format("csv").option("header", True).load(file_path)
        )
    logging.info(f"Successfully read file: {file_path}")
    return dataframe

def filter_column_by_list(dataframe: DataFrame, column_name: str, filter_list: list) -> DataFrame :
    """
    Filter a Spark DataFrame based on the condition that the values
    from the given list are present in the given column.

    :param dataframe: The DataFrame to be filtered.
    :param column_name: The column on which to filter.
    :param filter: The list of items that should be present in the column.

    :returns: The filtered dataframe.
    """
    # Error handling
    if column_name not in dataframe.columns:
        logging.info("Column name not available in dataframe.")
        raise ValueError()
    if not isinstance(filter_list, list):
        logging.info("provided filter_list is not a list")
        raise ValueError("Provided filter_list is not a list.")

    # Select the wanted rows using filter
    filtered_df = dataframe.filter(dataframe[column_name].isin(filter_list))
    logging.info(f"Data filtered on: {filter_list}")
    return filtered_df

def order_dataframe(df: DataFrame, column_name: str, ascending: bool = True) -> DataFrame:
    """
    Orders a PySpark DataFrame based on a given column.

    :param df: The input DataFrame to be ordered.
    :type df: DataFrame
    :param column_name: The name of the column to order by.
    :type column_name: str
    :param ascending: If True, sort in ascending order; otherwise, sort in descending order. Default is True.
    :type ascending: bool

    :return: The ordered DataFrame.
    :rtype: DataFrame

    :raises ValueError: If `column_name` is not in DataFrame columns.

    :example:

    >>> ordered_df = order_dataframe(data_df, 'sales_amount', ascending=False)
    >>> ordered_df.show()
    """
    if ascending:
        return df.orderBy(F.col(column_name).asc())
    else:
        return df.orderBy(F.col(column_name).desc())
    
def extract_pattern(df: DataFrame, new_column: str, extract_column: str, regex: str) -> DataFrame:
    """
    Extracts a pattern from a specified column in the DataFrame based on a given regular expression.

    :param df: The input DataFrame containing the data to process.
    :type df: DataFrame
    :param new_column: The name of the new column where the extracted pattern will be stored.
    :type new_column: str
    :param extract_column: The column containing the text to extract from.
    :type extract_column: str
    :param regex: The regular expression pattern to extract.
    :type regex: str

    :raises ValueError: If `extract_column` is not in DataFrame columns.

    :return: A DataFrame with an additional column containing the extracted pattern.
    :rtype: DataFrame

    :example:

    >>> updated_df = extract_pattern(data_df, 'postcode', 'address', r'(\d{4}\s?[A-Z]{2})')
    >>> updated_df.show()
    """
    if extract_column not in df.columns:
        logging.info("Column name not available in dataframe.")
        raise ValueError()
    # Extract pattern and create a new column called 'extracted_value'
    return df.withColumn(new_column, F.regexp_extract(F.col(extract_column), regex, 0))

def get_top_performers(df: DataFrame, group_by_col: str, order_by_col: str, top_n: int) -> DataFrame:
    """
    Ranks the top N performers in a DataFrame based on a specified ordering column.

    :param df: The input DataFrame containing the data to analyze.
    :type df: DataFrame
    :param group_by_col: The column to group by (e.g., department or area).
    :type group_by_col: str
    :param order_by_col: The column to order by (e.g., total sales amount).
    :type order_by_col: str
    :param top_n: The number of top performers to return.
    :type top_n: int

    :raises ValueError: If `group_by_col` or `order_by_col` is not in DataFrame columns.
    :raises ValueError: If `top_n` is not a positive integer.

    :return: A DataFrame containing the top N performers, ranked by the specified column.
    :rtype: DataFrame

    :example:

    >>> top_performers_df = get_top_performers(sales_df, 'department', 'total_sales', 3)
    >>> top_performers_df.show()
    """
    if group_by_col not in df.columns:
        logging.info("Column name '{group_by_col}' not available in dataframe.")
        raise ValueError("Parameter 'group_by_col' not available in dataframe.")
    if order_by_col not in df.columns:
        logging.info("Column name '{order_by_col}' not available in dataframe.")
        raise ValueError("Parameter 'order_by_col' not available in dataframe.")
    if not isinstance(top_n, int) or top_n <= 0:
        raise ValueError("Parameter 'top_n' must be a positive integer.")

    # Define the window specification
    window_spec = Window.partitionBy(group_by_col).orderBy(F.col(order_by_col).desc())

    # Rank the performers
    df_ranked = df.withColumn("rank", F.row_number().over(window_spec).cast(T.LongType()))

    # Filter for the top N performers
    df_top_performers = df_ranked.filter(F.col("rank") <= top_n)

    return df_top_performers

def main(dataset_one: str, dataset_two: str, dataset_three: str) -> None:

    setup_logging()

    dataset1 = dataset_one
    dataset2 = dataset_two
    dataset3 = dataset_three

    df1 = load_csv_in_spark(spark=spark, file_path=dataset1)
    df2 = load_csv_in_spark(spark=spark, file_path=dataset2)
    df3 = load_csv_in_spark(spark=spark, file_path=dataset3)

    logging.info("All datasets are loaded.")

    ## Output #1 - **IT Data**
    logging.info("Starting output 1")

    joining_list = ["id"]
    df_joined = df1.join(df2, joining_list)

    filtering_list= ["IT"]
    column_name= "area"
    df_filter = filter_column_by_list(
        dataframe= df_joined,
        column_name= column_name,
        filter_list= filtering_list
    )

    df_sorted = order_dataframe(df=df_filter, column_name="sales_amount", ascending=False)

    df_limit = df_sorted.limit(100)

    df_limit.write.format("csv").mode("overwrite").save("it_data/it_data.csv")

    logging.info("it_data.csv is created")

    logging.info("End of output 1")

    ### Output #2 - **Marketing Address Information**
    logging.info("Starting output 2")

    filtering_list = ["Marketing"]
    column_name= "area"
    df_marketing = filter_column_by_list(
        dataframe= df1,
        column_name= column_name,
        filter_list= filtering_list
    )

    df_postcode = extract_pattern(df=df2, new_column="postcode", extract_column="address", regex=r'(\d{4}\s?[A-Z]{2})')

    joining_list = ["id"]
    df_joined = df_marketing.join(df_postcode, joining_list)

    df_select = df_joined.select("id", "area", "name", "address", "postcode")

    df_select.write.format("csv").mode("overwrite").save("marketing_address_info/marketing_address_info.csv")

    logging.info("marketing_address_info.csv is created")

    logging.info("End of output 1")

    ### Output #3 - **Department Breakdown**
    logging.info("Starting output 3")

    joining_list = ["id"]
    df_joined = df1.join(df2, joining_list)

    df_aggregate = df_joined.groupBy(F.col("area"))\
        .agg(F.sum(F.col("sales_amount")).alias("total_sales_amount"),
            F.sum(F.col("calls_made")).cast(T.IntegerType()).alias("total_calls_made"),
            F.sum(F.col("calls_successful")).cast(T.IntegerType()).alias("total_calls_successful"))

    df_readable = df_aggregate.withColumn("total_sales_amount", F.format_number(F.col("total_sales_amount"), 0))

    df_percentage = df_readable.withColumn("success_calls_perc", (F.col("total_calls_successful")/F.col("total_calls_made")*100).cast(T.IntegerType()))

    df_percentage.write.format("csv").mode("overwrite").save("department_breakdown/department_breakdown.csv")

    logging.info("department_breakdown.csv is created")
    logging.info("Starting output 3")

    ### Output #4 - **Top 3 best performers per department**
    logging.info("Starting output 4")

    joining_list = ["id"]
    df_joined = df1.join(df2, joining_list)

    df_aggregate = df_joined.groupBy(F.col("area"), F.col("name"))\
        .agg(F.sum(F.col("sales_amount")).alias("total_sales_amount"),
            F.sum(F.col("calls_made")).cast(T.IntegerType()).alias("total_calls_made"),
            F.sum(F.col("calls_successful")).cast(T.IntegerType()).alias("total_calls_successful"))


    df_readable = df_aggregate.withColumn("total_sales_amount", F.format_number(F.col("total_sales_amount"), 0))

    df_percentage = df_readable.withColumn("success_calls_perc", (F.col("total_calls_successful")/F.col("total_calls_made")*100).cast(T.IntegerType()))

    df_filter = df_percentage.filter(F.col("total_calls_successful") > 75)

    df_top_performers = get_top_performers(df=df_filter, group_by_col="area", order_by_col="total_sales_amount", top_n=3)

    df_top_performers.write.format("csv").mode("overwrite").save("top_3/top_3.csv")

    logging.info("top_3.csv is created.")
    logging.info("End output 4")

    ## Output #5 - **Top 3 most sold products per department in the Netherlands**
    logging.info("Starting output 5")

    df_joined = df1.join(df3, df1.id == df3.caller_id)

    filtering_list= ["Netherlands"]
    column_name= "country"
    df_filter = filter_column_by_list(
        dataframe= df_joined,
        column_name= column_name,
        filter_list= filtering_list
    )

    df_aggregate = df_filter.groupBy(F.col("area"), F.col("product_sold"))\
        .agg(F.sum(F.col("quantity")).cast(T.IntegerType()).alias("total_quantity"))

    df_top3 = get_top_performers(df=df_aggregate, group_by_col="area", order_by_col="total_quantity", top_n=3)

    df_top3.write.format("csv").mode("overwrite").save("top_3_most_sold_per_department_netherlands/top_3_most_sold_per_department_netherlands.csv")

    logging.info("top_3_most_sold_per_department_netherlands.csv is created.")
    logging.info("End output 5")

    ## Output #6 - **Who is the best overall salesperson per country**
    logging.info("Starting output 6")

    df_joined = df2.join(df3, df2.id == df3.caller_id)

    df_aggregate = df_joined.groupBy(F.col("caller_id"), F.col("name"), F.col("country"))\
        .agg(F.sum(F.col("sales_amount")).cast(T.IntegerType()).alias("total_sales_amounty"))

    df_salesperson = get_top_performers(df=df_aggregate, group_by_col="country", order_by_col="total_sales_amounty", top_n=1)

    df_salesperson.write.format("csv").mode("overwrite").save("best_salesperson/best_salesperson.csv")

    logging.info("best_salesperson.csv is created.")
    logging.info("End of output 6")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        logging.error("Please provide paths to three dataset CSV files.")
        sys.exit(1)
    
    dataset_one_path = sys.argv[1]
    dataset_two_path = sys.argv[2]
    dataset_three_path = sys.argv[3]
    
    main(dataset_one_path, dataset_two_path, dataset_three_path)
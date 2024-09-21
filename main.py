from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
# from pyspark.sql.dataframe import DataFrame
# from pyspark.sql.window import Window
# from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
import sys
from app.functions import *

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

    df_limit.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save("it_data/it_data.csv")

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

    df_select.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save("marketing_address_info/marketing_address_info.csv")

    logging.info("marketing_address_info.csv is created")

    logging.info("End of output 2")

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

    df_percentage.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save("department_breakdown/department_breakdown.csv")

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

    df_top_performers.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save("top_3/top_3.csv")

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

    df_top3.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save("top_3_most_sold_per_department_netherlands/top_3_most_sold_per_department_netherlands.csv")

    logging.info("top_3_most_sold_per_department_netherlands.csv is created.")
    logging.info("End output 5")

    ## Output #6 - **Who is the best overall salesperson per country**
    logging.info("Starting output 6")

    df_joined = df2.join(df3, df2.id == df3.caller_id)

    df_aggregate = df_joined.groupBy(F.col("caller_id"), F.col("name"), F.col("country"))\
        .agg(F.sum(F.col("sales_amount")).cast(T.IntegerType()).alias("total_sales_amounty"))

    df_salesperson = get_top_performers(df=df_aggregate, group_by_col="country", order_by_col="total_sales_amounty", top_n=1)

    df_salesperson.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save("best_salesperson/best_salesperson.csv")

    logging.info("best_salesperson.csv is created.")
    logging.info("End of output 6")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        logging.error("Please provide paths to three dataset CSV files.")
        sys.exit(1)
    
    dataset_one_path = sys.argv[1]
    dataset_two_path = sys.argv[2]
    dataset_three_path = sys.argv[3]
    
    try:
        main(dataset_one_path, dataset_two_path, dataset_three_path)
    except Exception as e:
        logging.error(f"Error occurred during processing: {str(e)}")
        sys.exit(1)

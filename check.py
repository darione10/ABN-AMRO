from app import *

spark = (SparkSession.builder
  .master("local")
  .appName("test")
  .getOrCreate())

source_data = [
        (1, "IT", 54, 41),
        (2, "Marketing", 20, 1),
        (3, "Marketing", 15, 2),
        (4, "Games", 5, 1),
        (5, "Finance", 25, 2),
    ]

source_df = spark.createDataFrame(source_data, ["id","area","calls_made","calls_successful"])

d = filter_column_by_list(dataframe= source_df, column_name= "area", filter_list= ["IT"])

d.show()

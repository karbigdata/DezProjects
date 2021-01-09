# importing PySpark packages/APIs
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Demo - DataFrame from JSON File") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    file_full_path = "file:///D://apache_spark_dataframe//data//json//user_detail.json"

    df = spark.read.json(file_full_path)

    df.show(10, False)

    df.filter("user_id = 3").show(1, False)

    spark.stop()
    print("Completed.")
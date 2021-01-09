# importing PySpark packages/APIs
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Demo - Spark SQL") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    #file_full_path = "file:///D://apache_spark_dataframe//data//json//user_detail.json"
    file_full_path = "/data/json/user_dtl/user_detail.json"

    df = spark.read.json(file_full_path)

    df.show(10, False)

    # Creates a temporary view using the DataFrame
    df.createOrReplaceTempView("user")

    # SQL statements can be run by using the sql methods provided by spark
    users_df = spark.sql("SELECT name FROM user WHERE user_id BETWEEN 2 AND 4")
    users_df.show()

    spark.sql("SELECT * FROM user WHERE user_id BETWEEN 1 AND 4").show(10, False)

    spark.stop()
    print("Completed.")
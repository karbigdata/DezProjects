# importing PySpark packages/APIs
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Demo - DataFrame from CSV File") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    #file_full_path = "file:///D://apache_spark_dataframe//data//csv//transaction_detail.csv"
    file_full_path = "/data/csv/tran_dtl"

    # transaction_detail.csv file is place in the HDFS location: /data/csv
    #transaction_detail_df = spark.read.csv(file_full_path)
    transaction_detail_df = spark.read.option("header", True).option("inferSchema", True).csv(file_full_path)

    transaction_detail_df.show(10, False)

    transaction_detail_df.printSchema()

    # transaction_detail_df = spark.read.option("header", True).option("inferSchema", True).csv("/user/hadoop/data/pyspark_input_data/csv")

    transaction_detail_df.count()
    transaction_detail_df.show(2, False)
    transaction_detail_df_stg1 = transaction_detail_df.select("transaction_card_type", "transaction_country_name",
                                                              "transaction_amount")

    transaction_detail_df_stg1.show(5, False)

    transaction_detail_df_stg1.groupby('transaction_card_type').agg({'transaction_amount': 'sum'}).show()

    transaction_detail_df_stg1.groupby('transaction_country_name').agg({'transaction_amount': 'sum'}).show()

    # transaction_detail_df_stg1.groupby('transaction_country_name').agg({'transaction_amount': 'count'}).show()
    transaction_detail_df_stg1.groupby('transaction_country_name').count().show()

    transaction_detail_df_stg1.filter(transaction_detail_df_stg1.transaction_country_name == 'India').show(5, False)

    transaction_detail_df_stg1.select("transaction_country_name").distinct().show()

    spark.stop()
    print("Completed.")
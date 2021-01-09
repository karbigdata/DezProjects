package com.datamaking.spark.demo

// com.datamaking.spark.demo.spark_sql_demo

import org.apache.spark.sql.SparkSession

object spark_sql_demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Demo")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //Code Block 1 Starts Here
    //val json_file_path = "D:\\apache_spark_dataframe\\data\\json\\user_detail.json"
    val json_file_path = "/data/json/user_dtl/user_detail.json"

    val users_df_1 = spark.read.json(json_file_path)

    users_df_1.show(10, false)
    users_df_1.printSchema()

    // Register the DataFrame as a SQL temporary view
    users_df_1.createOrReplaceTempView("user")

    val sqlDF = spark.sql("SELECT * FROM user")
    sqlDF.show()

    spark.sql("SELECT * FROM user where user_id = 3").show()
    //Code Block 1 Ends Here
  }
}

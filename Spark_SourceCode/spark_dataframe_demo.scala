package com.datamaking.spark.demo

// com.datamaking.spark.demo.spark_dataframe_demo

import org.apache.spark.sql.types.{IntegerType, StructField, StructType, StringType}
import org.apache.spark.sql.{Row, SparkSession}

case class User(user_id: Int, user_name: String, user_city: String)

object spark_dataframe_demo {
  def main(args: Array[String]): Unit = {
    println("Application Started ...")
    val spark = SparkSession
      .builder()
      .appName("Create First Apache Spark DataFrame")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("Approach 1: ")
    // Code Block 1 Starts Here
    val users_list = List((1, "John", "London"), (2, "Martin", "New York"), (3, "Sam", "Sydney"), (4, "Alan", "Mexico City"), (5, "Jacob", "Florida"))

    // val users_df = spark.createDataFrame(spark.sparkContext.parallelize(users_list))

    val df_columns = Seq("user_id", "user_name", "user_city")
    val users_rdd = spark.sparkContext.parallelize(users_list)
    val users_df = spark.createDataFrame(users_rdd)
    users_df.show(5, false)
    println(users_df.getClass())

    val users_df_1 = users_df.toDF(df_columns:_*)
    println(users_df_1.getClass())
    users_df_1.show(5, false)
    // Code Block 1 Ends Here

    println("Approach 2: ")
    // Code Block 2 Starts Here
    val users_seq = Seq(Row(1, "John", "London"),
      Row(2, "Martin", "New York"),
      Row(3, "Sam", "Sydney"),
      Row(4, "Alan", "Mexico City"),
      Row(5, "Jacob", "Florida"))

    val users_schema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("user_city", StringType, true)
    ))

    val users_df_2 = spark.createDataFrame(spark.sparkContext.parallelize(users_seq), users_schema)

    users_df_2.show(5, false)
    // Code Block 2 Ends Here

    println("Approach 3: ")
    // Code Block 3 Starts Here
    val case_users_seq = Seq(User(1, "John", "London"),
      User(2, "Martin", "New York"),
      User(3, "Sam", "Sydney"),
      User(4, "Alan", "Mexico City"),
      User(5, "Jacob", "Florida"))

    val case_users_rdd = spark.sparkContext.parallelize(case_users_seq)
    val case_users_df = spark.createDataFrame(case_users_rdd)

    case_users_df.show(5, false)
    // Code Block 3 Ends Here

    //Code Block 4 Starts Here
    // val csv_comma_delimiter_file_path = "D:\\apache_spark_dataframe\\data\\csv\\user_detail_comma_delimiter.csv"
    val csv_comma_delimiter_file_path = "/data/csv/user_dtl/user_detail_comma_delimiter.csv"
    //val users_df_3 = spark.read.csv(csv_comma_delimiter_file_path)
    //val users_df_3 = spark.read.option("header", true).csv(csv_comma_delimiter_file_path)

    val users_df_3 = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(csv_comma_delimiter_file_path)

    users_df_3.show(10, false)
    users_df_3.printSchema()
    //Code Block 4 Ends Here

    spark.stop()
    println("Application Completed.")
  }
}

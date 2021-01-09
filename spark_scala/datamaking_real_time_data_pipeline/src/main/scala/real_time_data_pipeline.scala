package com.datamaking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

// com.datamaking.real_time_data_pipeline

object real_time_data_pipeline {
  def main(args: Array[String]): Unit = {
    println("Real-Time Data Pipeline Started ...")

    // Code Block 1 Starts Here
    val kafka_topic_name = "transmessage"
    val kafka_bootstrap_servers = "34.70.106.130:9092"

    val mongodb_host_name = "34.70.106.130"
    val mongodb_port_no = "27017"
    val mongodb_user_name = "admin"
    val mongodb_password = "admin"
    val mongodb_database_name = "meetup_rsvp_db"
    val mongodb_collection_name = "meetup_rsvp_message_detail_tbl"
    // Code Block 1 Ends Here

    // Code Block 2 Starts Here
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Real-Time Data Pipeline")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // Code Block 2 Ends Here

    // Code Block 3 Starts Here
    // eCommerce/Retail Message Data from Kafka
    val transaction_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic_name)
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()
    // Code Block 3 Ends Here

    // Code Block 4 Starts Here
    // Write final result into console for debugging purpose
    val trans_detail_write_stream = transaction_detail_df
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()
    // Code Block 4 Ends Here

    trans_detail_write_stream.awaitTermination()

    println("Real-Time Data Pipeline Completed.")
  }
}

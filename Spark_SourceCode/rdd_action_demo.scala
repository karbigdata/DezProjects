package com.datamaking.spark.demo

import org.apache.spark.sql.SparkSession

object rdd_action_demo {
  def main(args: Array[String]): Unit = {
    println("Started ...")
    val spark = SparkSession
      .builder
      .appName("Apache Spark Tutorial - RDD Actions")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val apache_spark_list = List("Apache", "Spark", "is", "in-memory", "distributed", "framework", "Spark")
    val apache_spark_rdd = spark.sparkContext.parallelize(apache_spark_list)
    val apache_spark_filter_rdd = apache_spark_rdd.filter(ele => ele.contains('a'))
    println("Printing the result: ")
    apache_spark_filter_rdd.collect().foreach(println)

    println("Printing the result of more action operation: ")
    apache_spark_rdd.take(2).foreach(println)
    println(apache_spark_rdd.first())

    val apache_spark_rdd_map = apache_spark_rdd.map(e => (e, 1))
    apache_spark_rdd_map.reduceByKey((a, b) => a + b).collect().foreach(println)

    spark.stop()
    println("Completed.")
  }
}

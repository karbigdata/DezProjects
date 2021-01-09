package com.datamaking.spark.demo

import org.apache.spark.sql.SparkSession

object rdd_transformation_demo {
  def main(args: Array[String]): Unit = {
    println("Started ...")
    val spark = SparkSession
      .builder
      .appName("Apache Spark Tutorial - RDD Transformations")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val numbers_list = 1 to 10
    print(numbers_list.getClass.getSimpleName)
    val numbers_rdd = spark.sparkContext.parallelize(numbers_list, 3)
    val numbers_mul_by_5_rdd = numbers_rdd.map(e => e * 5)
    println("Printing Numbers which are multiplied by 5: ")
    numbers_mul_by_5_rdd.collect().foreach(println)

    val apache_spark_list = List("Apache Spark is in-memory distributed framework",
      "Apache Hive is a distributed query engine using MapReduce framework")
    val apache_spark_rdd = spark.sparkContext.parallelize(apache_spark_list)
    val apache_spark_map_rdd = apache_spark_rdd.map(ele => ele.split(" "))
    println("Printing the result: ")
    // Arary[Array[String], Array[String]]
    apache_spark_map_rdd.collect().foreach(e => e.foreach(println))

    spark.stop()
    println("Completed.")
  }
}

# Importing Spark Related Packages
from pyspark.sql import SparkSession

# Importing Python Related Packages

if __name__ == "__main__":
    print("PySpark Tutorial")
    print("How to use filter RDD transformation in PySpark using PyCharm IDE")

    spark = SparkSession \
            .builder \
            .appName("How to filter map RDD transformation in PySpark") \
            .master("local[*]") \
            .enableHiveSupport() \
            .getOrCreate()

    py_number_list = [1, 2, 3, 4, 5]
    print("Printing Python Number List: ")
    print(py_number_list)

    print("Creating First RDD from Python Number List")

    number_rdd = spark.sparkContext.parallelize(py_number_list, 3)

    number_even_rdd = number_rdd.filter(lambda n: n % 2 == 0)

    print(number_even_rdd.collect())

    py_str_list = ["Arun", "Arvind", "Arjun", "Anna"]
    print(py_str_list)

    str_rdd = spark.sparkContext.parallelize(py_str_list, 2)

    str_rdd_result = str_rdd.filter(lambda name: 'r' in name).collect()
    print(str_rdd_result)

    input_file_path = "file:///D://datasets//sample_data//tech.txt"
    tech_rdd = spark.sparkContext.textFile(input_file_path)
    tech_lower_rdd = tech_rdd.filter(lambda ele: 'park' in ele)
    tech_lower_rdd_list = tech_lower_rdd.collect()

    for element in tech_lower_rdd_list:
        print(element)

    print("Stopping the SparkSession object")
    spark.stop()
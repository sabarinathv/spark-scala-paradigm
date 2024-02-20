package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

  object ConvertParquetToCsv extends App {

    // Creating spark session
    val spark: SparkSession = SparkSession.builder().master("local[3]").appName("scalabysab").getOrCreate()

    // Reading parquet file
    val df = spark.read.format("parquet").load("path_to_parquet_file/students.parquet")
    df.show()
    df.printSchema()

    // To do

  }

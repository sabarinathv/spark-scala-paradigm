package com.scalabysab.spark.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

  object ConvertParquetToCsv extends App {

    // Creating spark session
    val spark: SparkSession = SparkSession.builder().master("local[3]").appName("scalabysab").getOrCreate()

    // Reading Parquet file
    val df = spark.read.format("parquet").load("src/main/resources/cars.parquet")
    df.show()
    
    // Converting Parquet file to CSV
    df.write.mode(SaveMode.Overwrite).csv("/tmp/output_path/cars_output.csv")
    
  }

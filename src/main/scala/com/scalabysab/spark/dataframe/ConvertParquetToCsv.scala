package com.scalabysab.spark.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

  object ConvertParquetToCsv extends App {

    // Creating spark session
    val spark: SparkSession = SparkSession.builder().master("local[3]").appName("scalabysab").getOrCreate()

    // Reading Parquet file
    val df = spark.read.format("parquet").load("path_to_parquet_file/students_details.parquet")
    df.show()
    
    // Converting Parquet file to Csv
    df.write.mode(SaveMode.Overwrite).csv("/tmp/output/students_details.csv")
    
  }

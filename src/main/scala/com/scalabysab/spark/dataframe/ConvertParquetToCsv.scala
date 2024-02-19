package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

  object ConvertParquetToCsv extends App {

    // Creating spark session
    val spark: SparkSession = SparkSession.builder().master("local[3]").appName("scalabysab").getOrCreate()

    // To do

  }

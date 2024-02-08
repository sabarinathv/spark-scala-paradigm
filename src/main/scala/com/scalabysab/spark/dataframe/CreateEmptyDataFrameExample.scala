package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

  object CreateEmptyDataFrameExample extends App {

    val spark: SparkSession = SparkSession.builder().master("local[1]").appName("scalabysab").getOrCreate()

    // To do

  }


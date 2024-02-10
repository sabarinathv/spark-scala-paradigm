package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

object ReadCSVUsingDataFrame {

  // Creating SparkSession
  val spark:SparkSession = SparkSession.builder().master("local[5]").appName("sparkscalabysab").getOrCreate()

  // To do
  
  }
}

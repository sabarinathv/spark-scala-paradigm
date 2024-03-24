package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

object ReadJsonFileUsingDataframe {

  // Creating SparkSession
  val spark:SparkSession = SparkSession.builder().master("local[3]").appName("sparkscalabysab").getOrCreate()
 
  // To do
  
  }
}

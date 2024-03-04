package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

object ReadTextFileUsingDataframe {

  // Creating SparkSession
  val spark:SparkSession = SparkSession.builder().master("local[5]").appName("sparkscalabysab").getOrCreate()

  // Reading from CSV file
  val df = spark.read.csv("src/main/resources/sample1.txt")
  df.show()
  df.printSchema()

  // To do
  
  }
}

package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

object ReadTextFileUsingDataframe {

  // Creating SparkSession
  val spark:SparkSession = SparkSession.builder().master("local[3]").appName("sparkscalabysab").getOrCreate()

  // Reading from text file
  val df = spark.read.text("src/main/resources/sample1.txt")
  df.show()
  df.printSchema()

  // To do
  
  }
}

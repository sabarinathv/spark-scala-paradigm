package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

object ReadJsonFileUsingDataframe {

  // Creating SparkSession
  val spark:SparkSession = SparkSession.builder().master("local[3]").appName("sparkscalabysab").getOrCreate()

  // Reading Json file using dataframe
  val df = spark.read.json("src/main/resources/cars.json")
  df.printSchema()
  df.show(false)

  // Reading Multiline Json file using dataframe
  val multilineJsonDf = spark.read.option("multiline", "true").json("src/main/resources/<filename>")
  multilineJsonDf.printSchema()
  multilineJsonDf.show(false)
 
  // To do
  
  }
}

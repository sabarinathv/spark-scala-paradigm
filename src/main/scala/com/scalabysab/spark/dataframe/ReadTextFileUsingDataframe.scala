package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

object ReadTextFileUsingDataframe {

  // Creating SparkSession
  val spark:SparkSession = SparkSession.builder().master("local[3]").appName("sparkscalabysab").getOrCreate()

  // Reading from text file
  val dataFrame = spark.read.text("src/main/resources/text/sample1.txt")
  ddataFramef.show()
  dataFrame.printSchema()

  // Reading from text file using Spark Dataset
  val dataSet:Dataset[String] = spark.read.textFile("src/main/resources/text/sample1.txt")
  dataSet.show()
  dataSet.printSchema()
  
  // To do
  
  }
}

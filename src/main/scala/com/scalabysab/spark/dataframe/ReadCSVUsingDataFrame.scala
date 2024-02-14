package com.scalabysab.spark.dataframe

import org.apache.spark.sql.SparkSession

object ReadCSVUsingDataFrame {

  // Creating SparkSession
  val spark:SparkSession = SparkSession.builder().master("local[5]").appName("sparkscalabysab").getOrCreate()

  // Reading from CSV file
  val df1 = spark.read.csv("src/main/resources/student_records_with_header.csv")
  df1.show()
  df1.printSchema()

  // Reading from CSV file with options
  val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("src/main/resources/student_records_with_header.csv")
  df2.show()
  df2.printSchema()
  
  // To do
  
  }
}

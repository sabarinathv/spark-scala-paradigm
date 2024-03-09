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

  // Creating schema and reading through it
  import org.apache.spark.sql.types._
  val schema = new StructType().add("RollNumber",IntegerType,true).add("Name",StringType,true).add("DateOfBirth",StringType,true).add("Gender",StringType,true)
  
  // Creating dataframe with the CSV file, schema attched
  val dFWithSchema = spark.read.format("csv").option("header", "true").schema(schema).load("src/main/resources/student_records_with_header.csv")
  dFWithSchema.printSchema()
  dFWithSchema.show(false)

 // Wrting the output as a CSV file
 dFWithSchema.write.mode(SaveMode.Overwrite).csv("c:/tmp/output_path/student_records_output")

  
  }
}

package com.scalabysab.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

  object CreateEmptyDataFrame extends App {

    // Creating SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[3]").appName("scalabysab").getOrCreate()

    // Importing spark implicits
    import spark.implicits._

    // Creating schema
    val schema = StructType( StructField("firstName", StringType, true) ::
                              StructField("lastName", IntegerType, false) ::)

    val colSeq = Seq("firstName","lastName")

    // Creating case class
    case class Name(firstName: String, lastName: String)

    // Empty dataframe creation
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    // Using spark implicits - option 1
    Seq.empty[(String,String)].toDF(colSeq:_*)

    // Case class implementation - option 2
    Seq.empty[Name].toDF().printSchema()

    // Using emptyDataFrame function - option 3
    spark.emptyDataFrame


  }


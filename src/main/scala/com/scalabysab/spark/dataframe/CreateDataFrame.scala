package com.scalabysab.spark.dataframe

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CreateDataFrame {

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder().master("local[5]").appName("scalabysab").getOrCreate()

    import spark.implicits._
    val columns = Seq("Cars","number_of_users")
    val value = Seq(("BMW", "1200"), ("AUDI", "2100"), ("BENZ", "3600"))
    val rdd = spark.sparkContext.parallelize(value)

    //RDD to DF - option 1
    val rddToDf1 = rdd.toDF("Car","Count")
    rddToDf1.printSchema()

    //RDD to DF - option 2
    val rddToDf2 = spark.createDataFrame(rdd).toDF(columns:_*)
    rddToDf2.printSchema()

    //Creating schema
    val schema = StructType( Array(StructField("Car", StringType,  true), StructField("Count", StringType,  true)))

    val rowRdd = rdd.map(attributes => Row(attributes._1, attributes._2))
    val rddToDfWithSchema = spark.createDataFrame(rowRdd,schema)

    //From Value - option 1
    val dfFromValue1 = value.toDF()

    //From Value - option 2
    var dfFromValue2 = spark.createDataFrame(value).toDF(columns:_*)

    //From Value by adding schema
    import scala.collection.JavaConversions._
    val rowData = value.map(attributes => Row(attributes._1, attributes._2))
    var dfFromValue3 = spark.createDataFrame(rowData,schema)

  }
}

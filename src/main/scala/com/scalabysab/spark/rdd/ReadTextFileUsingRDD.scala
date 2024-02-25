package com.scalabysab.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadCSVFileUsingRDD {

  def main(args:Array[String]): Unit ={

    // creating spark session
    val spark:SparkSession = SparkSession.builder().master("local[3]").appName("sparkscalabysab").getOrCreate()
    val sc = spark.sparkContext

    
  }

}

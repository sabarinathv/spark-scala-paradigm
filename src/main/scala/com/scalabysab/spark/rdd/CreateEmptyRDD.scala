package com.scalabysab.spark.rdd

import org.apache.spark.sql.SparkSession

object CreateEmptyRDD extends App{

  // creating spark session

  val spark:SparkSession = SparkSession.builder().master("local[5]").appName("sparkscalabysab").getOrCreate()

  // creating rdd's

  val rdd1 = spark.sparkContext.emptyRDD
  val rddString = spark.sparkContext.emptyRDD[String]
  println(rdd1 + " *** " + rddString)
  println("Number of Partitions -- " + rdd1.getNumPartitions)
  rddString.saveAsTextFile("c:/tmp_test/test1.txt")

  val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
  println(rdd2)
  println("Number of Partitions -- " + rdd2.getNumPartitions)
  rdd2.saveAsTextFile("c:/tmp_test/test2.txt")

  type dataType = (String,Int)
  var pairRDD = spark.sparkContext.emptyRDD[dataType]
  println(pairRDD)

}

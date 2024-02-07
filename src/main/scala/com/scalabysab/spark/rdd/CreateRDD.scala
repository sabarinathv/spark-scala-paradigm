package com.scalabysab.spark.rdd

import org.apache.spark.sql.SparkSession

object CreateRDD {

  def main(args:Array[String]): Unit ={

    // creating spark session

    val spark:SparkSession = SparkSession.builder().master("local[5]").appName("sparkscalabysab.com").getOrCreate()

    // creating rdd's

    val rdd1=spark.sparkContext.parallelize(Seq(("Cat", 15), ("Dog", 30), ("Cow", 22)))
    rdd1.foreach(println)

    val rdd2 = spark.sparkContext.textFile("/path_to_file/textFile.txt")

    val rdd3 = spark.sparkContext.wholeTextFiles("/path_to_file/textFile.txt")
    rdd3.foreach(record=>println("FileName -- " + record._1+ ", FileContents -- " + record._2))

    val rdd4 = rdd.map(row=>{(row._1,row._2+100)})
    rdd4.foreach(println)

    val rdd5 = spark.range(20).toDF().rdd
    rdd5.foreach(println)

  }
}

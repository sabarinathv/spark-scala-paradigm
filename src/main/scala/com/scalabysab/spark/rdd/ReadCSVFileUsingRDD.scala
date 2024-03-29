package com.scalabysab.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadCSVFileUsingRDD {

  def main(args:Array[String]): Unit ={

    // creating spark session
    val spark:SparkSession = SparkSession.builder().master("local[5]").appName("sparkscalabysab").getOrCreate()
    val sc = spark.sparkContext

    //Reading CSV File using rdd
    val rdd1 = sc.textFile("src/main/resources/student_records.csv")

    // creating case class
    case class StudentDetail(rollno: String, fullName: String, dateOfBirth: String, gender: String)

    // spliting the strings
    val rdd2:RDD[StudentDetail] = rdd1.map(row=>{
     val str = splitStr(row)
      StudentDetail(str(0),str(1),str(2),str(3))
    })

    rdd2.foreach(x=>println(x.fullName))

    // function to split strings
    def splitStr(row:String):Array[String]={
      row.split(",")
    }
    
  }

}

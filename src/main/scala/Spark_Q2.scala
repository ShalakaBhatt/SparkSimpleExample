package com.sparkscala.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Spark_Q2 {


    def main(args : Array[String])   ={

      val spark = SparkSession
        .builder()
        .appName("Spark_Q2")
        .master("local[2]")
        //.enableHiveSupport()
        .getOrCreate()

      val aadhar_auth = spark.sqlContext.read.format("csv").option("header","true").load("/Users/shalakabhatt/Documents/SocGen/").select("sa","aua","res_state_name")
      //aadhar_auth.printSchema()
      //aadhar_auth.show()

      aadhar_auth.createOrReplaceTempView("ad_auth")

      val aadhar_auth1 = spark.sqlContext.sql(""" select * from ad_auth where aua > 650000 and res_state_name != "Delhi" """)
      aadhar_auth1.show()


    }

}

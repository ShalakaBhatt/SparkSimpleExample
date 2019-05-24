package com.sparkscala.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object AggExample {

  case class Student(sId: Long, name: String, age: Int, gender: String)
  
  def main(args : Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("AggExample")
      .master("local[2]")
      //.enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val students = List(Student(1,"James",21,"M"),Student(2,"Jane",25,"F"),Student(3,"David",22,"M"),
      Student(4,"Rita",25,"F"),Student(5,"Robert",21,"M"))

    val students1 = spark.sparkContext.parallelize(students)
    val studentsDF=students1.toDF()
    //studentsDF.printSchema()
    //studentsDF.explain(true)
    //studentsDF.show(true)
    studentsDF.show()

    studentsDF.createOrReplaceTempView("student")
    val std1=spark.sql("""select * from student""")
    println("********Result for student table*******")
    std1.show()
    val std2=std1.agg(max("age").as("Max_Age"))
    std2.show()

    std1.write.parquet("/Users/shalakabhatt/std2")
  }

}

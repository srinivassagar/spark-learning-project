package com.training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object FileGenerator extends App {

  println("hello world")
  /*
  //Load a file into dataframe
  //add two column integer values and create a new column
  //group by on student name and get total score
  //same output save it another file
   */

  /*
  First create a sparkSession
   */

  val spark = SparkSession.
    builder().
    appName("Student data Analytics").
    master("spark://spark:7077").getOrCreate()

  println(spark.sparkContext.isLocal)
  println(spark.sparkContext.isStopped)

  val students_df:DataFrame = spark.
    read.
    option("header","true").
    option("inferSchema","true").
    csv("/Users/sria/Downloads/Student_data.csv")

  val students_df_2 = spark. read.
    option("header","true").
    option("inferSchema","true").
    csv("/Users/sria/Downloads/Student_data2.csv")

  students_df.createOrReplaceTempView("student_score")
  students_df_2.createOrReplaceTempView("student_score2")

  val sql_df:DataFrame = spark.sql("select * from student_score")

  val sql_df2:DataFrame = spark.sql("select * from student_score2")

  val join_df = spark.sql("select * from student_score s1 join student_score2 s2 on s1.id=s2.id")

  val join_df2=  spark.sql("select * from student_score s1 join student_score2 s2 on s1.id=s2.id")

  join_df.join(join_df2,"id")

  //join_df.show(100,false)


  //spark.sql("select student_score.name from student_score join table2 on std_sc .id = table2.id")

  //val student_total_scores = spark.sql("select name,sum(score) as total_score from student_score group by name order by sum(score) desc ")

  //sql_df.show(100,false)
  //student_total_scores.show(100,false)

  //students_df.printSchema()

  //val students_df:DataFrame = spark.read.format("csv").option()

  //students_df.show(100,false)

  Thread.sleep(200000)
}

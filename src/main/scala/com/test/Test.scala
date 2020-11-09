package com.test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Try
object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    val name = "hello"
    val sd = spark.udf.register("sd", (c: String) => {

      TestCode.NCODE(c)

    })

    // apply(c:String)= TestCode.NCODE(c)

    //import org.apache.commons.lang3.StringUtils

    val (fun, argumentTypes, returnType) = ScalaGenerateFunsV2(
      """
        |def apply(c:String)= TestCode.NCODE(c)
        |""".stripMargin
    ,
      """
        |import com.umisen.jobs.dynamicudf.TestCode
        |""".stripMargin
    )

    val inputTypes = Try(argumentTypes.toList).toOption

    def builder(e: Seq[Expression]) = ScalaUDF(fun, returnType, e, inputTypes.getOrElse(Nil), Some(name))

    spark.sessionState.functionRegistry.registerFunction(name, builder)

    val rdd = spark
      .sparkContext
      .parallelize(Array(("dounine", "20","1"),("marry", "24","0")))
      .map(x => Row.fromSeq(Array(x._1, x._2, x._3)))

    val types = StructType(
      Array(
        StructField("name", StringType),
        StructField("age", StringType),
        StructField("gender", StringType)
      )
    )


    val rdd2 = spark
      .sparkContext
      .parallelize(Array(("dounine", 20,"1"),("marry", 20,"0")))
      .map(x => Row.fromSeq(Array(x._1, x._2,x._3)))

    val types2 = StructType(
      Array(
        StructField("name1", StringType),
        StructField("age1", IntegerType),
        StructField("gender1", StringType)
      )
    )

    val frame = spark.createDataFrame(rdd, types)

    val frame2 = spark.createDataFrame(rdd2, types2)


    frame.union(frame2).show()

    frame.createTempView("log")

    //frame.withColumn("gender",sd(col("gender"))).show(10)


    spark.sql("select hello(gender) from log").show(false)

  }

}

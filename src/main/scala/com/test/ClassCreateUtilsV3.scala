package com.test

import java.lang.reflect.Method

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox


case class ClassInfoV3(clazz: Class[_], instance: Any, defaultMethod: Method, methods: Map[String, Method], func: String) {
  def invoke[T](args: Object*): T = {
    defaultMethod.invoke(instance, args: _*).asInstanceOf[T]
  }
}

object ClassCreateUtilsV3 {
  private val clazzs = new java.util.HashMap[String, ClassInfoV3]()
  private val classLoader = scala.reflect.runtime.universe.getClass.getClassLoader
  private val toolBox = universe.runtimeMirror(classLoader).mkToolBox()

  def apply(className: String, classEntity: String, imports: String): ClassInfoV3 = this.synchronized {
    var clazz = clazzs.get(className)
    if (clazz == null) {
      val classBody = wrapClass(classEntity, imports)
      val zz = compile(prepareScala(className, classBody))
      val defaultMethod = zz.getDeclaredMethods.head
      val methods = zz.getDeclaredMethods


      val value = zz.getDeclaredConstructor(classOf[Map[_, _]])
      value.setAccessible(true)

      clazz = ClassInfoV3(
        zz,
        value.newInstance(Map("0" -> "male", "1" -> "female")),
        defaultMethod,
        methods = methods.map { m => (m.getName, m) }.toMap,
        className
      )
      clazzs.put(className, clazz)
      println(s"dynamic load class => $clazz")
    }
    clazz
  }

  def compile(src: String): Class[_] = {
    val tree = toolBox.parse(src)
    toolBox.compile(tree).apply().asInstanceOf[Class[_]]
  }

  def prepareScala(className: String, classBody: String): String = {
    classBody + "\n" + s"scala.reflect.classTag[$className].runtimeClass"
  }

  def wrapClass(classEntity: String, imports: String): (String) = {
    val classBody =
      s"""
         |  $imports
         |  $classEntity
            """.stripMargin
    (classBody)
  }

  def main(args: Array[String]): Unit = {
    val infos = ClassCreateUtilsV3(
      "FilterUdf1",
      """
        |class FilterUdf1 extends UDF1[String, String] with Serializable {
        |
        |  var wordTrieList: Map[String,String] = _
        |
        |  val log: Logger = LoggerFactory.getLogger(classOf[FilterUdf1])
        |
        |  def this (words: Map[String,String]) {
        |    this ()    //调用主构造函数
        |    this.wordTrieList = words
        |  }
        |
        |  override def call(stringSeq: String): String = {
        |    wordTrieList(stringSeq)
        |  }
        |
        |}
      """.stripMargin,
      """
        |import org.apache.spark.sql.api.java.UDF1
        |import org.slf4j.{Logger, LoggerFactory}
      """.stripMargin
    )

    //println(infos.defaultMethod.invoke(infos.instance, "dounine 本猪会一点点 spark"))
    println(infos.methods("call").invoke(infos.instance, "0"))

  }

}


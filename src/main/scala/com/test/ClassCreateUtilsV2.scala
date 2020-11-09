package com.test

import java.util.UUID

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox


/*case class ClassInfo(clazz: Class[_], instance: Any, defaultMethod: Method, methods: Map[String, Method], func:String) {
  def invoke[T](args: Object*): T = {
    defaultMethod.invoke(instance, args: _*).asInstanceOf[T]
  }
}*/
object ClassCreateUtilsV2{
  private val clazzs = new java.util.HashMap[String, ClassInfo]()
  private val classLoader = scala.reflect.runtime.universe.getClass.getClassLoader
  private val toolBox = universe.runtimeMirror(classLoader).mkToolBox()
  def apply(func: String,imports:String): ClassInfo = this.synchronized {
    var clazz = clazzs.get(func)
    if (clazz == null) {
      val (className, classBody) = wrapClass(func,imports)
      val zz = compile(prepareScala(className, classBody))
      val defaultMethod = zz.getDeclaredMethods.head
      val methods = zz.getDeclaredMethods
      clazz = ClassInfo(
        zz,
        zz.newInstance(),
        defaultMethod,
        methods = methods.map { m => (m.getName, m) }.toMap,
        func
      )
      clazzs.put(func, clazz)
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
  def wrapClass(function: String,imports:String): (String, String) = {
    val className = s"dynamic_class_${UUID.randomUUID().toString.replaceAll("-", "")}"
    val classBody =
      s"""
         |class $className{
         |  $imports
         |  $function
         |}
            """.stripMargin
    (className, classBody)
  }

  def main(args: Array[String]): Unit = {
    val infos = ClassCreateUtilsV2(
      """
        |def apply(c:String)= StringUtils.capitalize(c)
        |def apply22(name:String)=name.toLowerCase
      """.stripMargin,
      """
        |import org.apache.commons.lang3.StringUtils
      """.stripMargin
    )

    println(infos.defaultMethod.invoke(infos.instance,"dounine 本猪会一点点 spark"))
    println(infos.methods("apply").invoke(infos.instance,"dounine 本猪会一点点 spark"))
    println(infos.methods("apply22").invoke(infos.instance,"ASDASDAS 本猪会一点点 ASDASDA"))

  }

}


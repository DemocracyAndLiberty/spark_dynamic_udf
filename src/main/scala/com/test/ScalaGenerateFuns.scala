package com.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.types.DataType

object ScalaGenerateFuns {

  def apply(func: String): (AnyRef, Array[DataType], DataType) = {
    val (argumentTypes, returnType) = getFunctionReturnType(func)
    (generateFunction(func, argumentTypes.length), argumentTypes, returnType)
  }

  //获取方法的参数类型及返回类型
  private def getFunctionReturnType(func: String): (Array[DataType], DataType) = {
    val classInfo = ClassCreateUtils(func)
    val method = classInfo.defaultMethod
    val dataType = JavaTypeInference.inferDataType(method.getReturnType)._1
    (method.getParameterTypes.map(JavaTypeInference.inferDataType).map(_._1), dataType)
  }

  //生成22个Function
  def generateFunction(func: String, argumentsNum: Int): AnyRef = {
    lazy val instance = ClassCreateUtils(func).instance
    lazy val method = ClassCreateUtils(func).methods("apply")

    argumentsNum match {
      case 0 => new (() => Any) with Serializable with Logging {
        override def apply(): Any = {
          try {
            method.invoke(instance)
          } catch {
            case e: Exception =>
              logError(e.getMessage)
          }
        }
      }
      case 1 => new (Object => Any) with Serializable with Logging {
        override def apply(v1: Object): Any = {
          try {
            method.invoke(instance, v1)
          } catch {
            case e: Exception =>
              e.printStackTrace()
              logError(e.getMessage)
              null
          }
        }
      }
      case 2 => new ((Object, Object) => Any) with Serializable with Logging {
        override def apply(v1: Object, v2: Object): Any = {
          try {
            method.invoke(instance, v1, v2)
          } catch {
            case e: Exception =>
              logError(e.getMessage)
              null
          }
        }
      }
      /* 完整匿名类写法
      case 3 => new Function3[Object, Object,Object,Any] with Serializable with Logging {
         override def apply(v1: Object, v2: Object, v3: Object): Any = v1.toString+v2+v3
       }*/
      //... 麻烦大佬自己去写剩下的20个了，这里装不下了，不然浏览器会崩溃的，然后电脑会重启的，为了大佬的电脑着想。
    }
  }

}
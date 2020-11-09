package com.test

import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.api.java.UDF1


class FilterUdf1 extends UDF1[String, String] with Serializable {

  var wordTrieList: Map[String,String] = _

  val log: Logger = LoggerFactory.getLogger(classOf[FilterUdf1])

  def this (words: Map[String,String]) {
    this ()    //调用主构造函数
    this.wordTrieList = words
  }

  override def call(stringSeq: String): String = {
    wordTrieList(stringSeq)
  }

}

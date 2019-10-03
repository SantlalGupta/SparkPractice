package com.batch.core

object testt {
  def main(args: Array[String]): Unit = {
    val str = "[u'Salman',u'Sarukh']"

     val ar:Array[String] = str.substring(1, str.length - 1).split(",").map(x=>x.substring(2,x.length-1))
    ar.foreach(println)
  }
}

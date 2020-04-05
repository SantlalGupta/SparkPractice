package com.analytics

import java.io.{IOException, InputStream}
import java.util.Properties
import org.slf4j.LoggerFactory


class PropertyLoader {
  var inputStream: InputStream = null
  val properties = new Properties

  private val logger = LoggerFactory.getLogger(classOf[PropertyLoader].getName)

  private def loadProperty(propertyFile: String): Unit = {
    try {
      inputStream = classOf[PropertyLoader].getClassLoader.getResourceAsStream(propertyFile)
      if (inputStream != null) properties.load(inputStream)
    } catch {
      case e: Exception =>
        logger.error("Exception raised while loading property file : " + propertyFile + " \n" + "Exception : " + e)
    } finally try inputStream.close()
    catch {
      case e: IOException =>
        System.out.println("Exception raised while closing input stream : " + e)
    }
  }

  def getMapProperties(propertyFile: String) = {
    loadProperty(propertyFile)
    import scala.collection.JavaConverters._
    properties.asScala.toMap

  }
}

//object test {
//  def main(args: Array[String]): Unit = {
//    println(new PropertyLoader().getMapProperties("config.properties").get(new MysqlProperty().BATCHSIZE).get)
//  }
//}
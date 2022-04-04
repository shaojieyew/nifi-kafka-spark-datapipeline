package app

import org.apache.spark.sql.SparkSession
import common.ConfigLoader

trait BaseApp {
  var sparkSession:SparkSession = null
  var appConfig: ConfigLoader.App = null
  var kafkaHosts: String = ""
  def process()

  def main(args: Array[String]) {
    if(args.length==1){
      System.exit(1)
    }
    val configPath = args(0)
    val appName = args(1)

    sparkSession = SparkSession.builder().appName(appName)
      .getOrCreate()

    val config = ConfigLoader.load(configPath)
    val appConfigs = config.jobs.filter(_.name.equals(appName))

    appConfigs match {
      case Nil => throw new Exception(s"Job name $appName not found")
      case _ => {
        kafkaHosts = config.kafkahosts.mkString(",")
        appConfig = appConfigs.length match {
          case 1 => appConfigs(0)
          case _ => null
        }
        process()
      }
    }

  }

}



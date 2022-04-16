package common

import org.apache.spark.sql.SparkSession

trait BaseApp {

  var configPath: String = _
  var appName: String = _

  lazy val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master("local[*]")
    .getOrCreate()

  lazy val appConfig: ConfigLoader.AppConfig = ConfigLoader.load(configPath, appName)

  def main(args: Array[String]) {
    if (args.length < 2) {
      throw new Exception(s"config file path and app name is not provided")
    }
    configPath = args(0)
    appName = args(1)
    process()

  }

  def process()

}

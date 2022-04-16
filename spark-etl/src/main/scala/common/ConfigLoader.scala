package common

import cats.syntax.either._
import io.circe.generic.auto._
import java.io.{FileInputStream, InputStreamReader}

import io.circe.{Error, yaml}


object ConfigLoader {
  case class Config(kafkaHosts: List[String],
                    jobs: List[AppConfig])

  case class AppConfig(name:Option[String],
                       checkpoint: Option[String],
                       input: Option[String],
                       output: Option[String],
                       startingOffsets: Option[String],
                       kafkaHosts: Option[String])

  def load(path: String, appName: String): AppConfig ={
    val yml = yaml.parser.parse(new InputStreamReader(new FileInputStream(path)))

    val config = yml.leftMap(err => err: Error)
      .flatMap(_.as[Config])
      .valueOr(throw _)

    val appConfigs = config.jobs.filter(_.name.get == appName)

    appConfigs match {
      case Nil => throw new Exception(s"Job name $appName not found")
      case _ => {
        appConfigs.length match {
          case 1 => appConfigs.head.copy(kafkaHosts = Some(config.kafkaHosts.mkString(",")))
          case _ => throw new Exception(s"There are multiple config with same name, $appName")
        }
      }
    }
  }
}
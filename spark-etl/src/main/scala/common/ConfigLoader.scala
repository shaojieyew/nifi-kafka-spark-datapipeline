package common

import cats.syntax.either._
import io.circe.generic.auto._
import java.io.{FileInputStream, InputStreamReader}

import io.circe.{Error, yaml}


object ConfigLoader {
  case class Config(kafkahosts: List[String],
                    jobs: List[App])
  case class App(name:String,
                 checkpoint: String,
                 topicIn: String,
                 topicOut: String,
                 startingOffsets: String)

  def load(path: String): Config ={
    val config = new FileInputStream(path);;
    val yml = yaml.parser.parse(new InputStreamReader(config))

    yml.leftMap(err => err: Error)
      .flatMap(_.as[Config])
      .valueOr(throw _)
  }
}
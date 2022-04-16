package app


import com.google.protobuf.timestamp.Timestamp
import common.BaseApp
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scalapb.protos.kafka.Sms


/**
 * Implement a Streaming ETL that reads SMS records from Kafka stream,
 * transform and writes the Word Count result as CSV into a directory
 */

object StreamingSmsWordCountEtl extends BaseApp {

  case class WordCount(word: Option[String],
                       spam: Option[Boolean],
                       smsTimestamp: Option[Timestamp],
                       frequency: Option[Int])

  def process(): Unit = {

    import spark.implicits._
    val streamingDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", appConfig.kafkaHosts.get)
      .option("subscribePattern", appConfig.input.get )
      .option("startingOffsets", appConfig.startingOffsets.get)
      .load()
      .select("value").as[Array[Byte]]
      .map(Sms.parseFrom)

    /**
     * word:	string	- tokernised and normalised to lower case from content
     * spam:	boolean - true if the sender is a spam number
     * sms_timestamp:	timestamp -	round down to 5 min interval
     * frequency:	int	- count of the word used within the 5 min interval that are either spam or not spam
     *
     * There are no primary key, so its ok to have duplicate records.
     */

    // TODO implement streaming word count
    val mocResultDf = streamingDf.map(record=>{
      WordCount(word = Some("DUMMY"),
        spam = Some(true),
        smsTimestamp = record.smsTimestamp,
        frequency = Some(1))
    })


    mocResultDf.writeStream.format("csv")
            .trigger(Trigger.ProcessingTime(30 * 1000L))
            .outputMode(OutputMode.Append())
            .option("checkpointLocation", appConfig.checkpoint.get)
            .start(appConfig.output.get)
            .awaitTermination()

  }
}

package app

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import java.io.{IOException, StringReader}
import java.time.Instant

import scalapb.protos.kafka.{RawMessage, Sms}
import org.apache.commons.csv.CSVFormat
import com.google.protobuf.timestamp.Timestamp
object SmsEtl extends BaseApp {

  case class KafkaMessage(value: Array[Byte], key:  Array[Byte], topic: String, parition: Int, offset: Long)

  def process(): Unit = {
    val spark = sparkSession;

    import spark.implicits._

    val streamingDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHosts)
      .option("subscribePattern", appConfig.topicIn )
      .option("startingOffsets", appConfig.startingOffsets)
      .load().as[KafkaMessage]

    def readCsvRecords(header: String,content: String, delimiter: Char = ',') = {
      CSVFormat.RFC4180.withIgnoreSurroundingSpaces
        .withIgnoreEmptyLines
        .withTrim
        .withHeader(header.split(delimiter): _*)
        .withDelimiter(delimiter)
        .parse(new StringReader(content))
    }

    val resultDf = streamingDf
      .map(_.value)
      .map(RawMessage.parseFrom)
      .map(record =>{
      val r = readCsvRecords(record.schema, record.content).getRecords.get(0)
      Sms(
        sender = r.get("sender"), recipient = r.get("recipient"),
        senderNorm = r.get("sender").filter(_.isDigit),
        recipientNorm =  r.get("recipient").filter(_.isDigit),
        smsTimestamp = Some(Timestamp(r.get("timestamp").toLong)),
        processedTimestamp = Some(Timestamp(Instant.now.getEpochSecond)),
        country = r.get("country"),
        content = r.get("content"),
        countryName = ???,
        latitude = ???,
        longitude = ???
      )
    })

    resultDf.writeStream.format("kafka")
            .trigger(Trigger.ProcessingTime(30 * 1000L))
            .outputMode(OutputMode.Append())
            .option("checkpointLocation", appConfig.checkpoint)
            .start()
            .awaitTermination()

  }
}

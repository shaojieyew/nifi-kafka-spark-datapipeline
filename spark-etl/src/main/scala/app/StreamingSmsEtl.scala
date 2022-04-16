package app

import java.io.StringReader
import java.time.Instant

import com.google.protobuf.timestamp.Timestamp
import common.BaseApp
import org.apache.commons.csv.CSVFormat
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scalapb.protos.kafka.{RawMessage, Sms}

/**
 * Implement a Streaming ETL that reads SMS records from Kafka stream, enrich it and write the result into another Kafka Topic
 */

object StreamingSmsEtl extends BaseApp {

  case class KafkaMessage(value: Array[Byte],
                          key:  Array[Byte],
                          topic: String,
                          partition: Int, offset: Long)

  def process(): Unit = {

    import spark.implicits._

    val streamingDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", appConfig.kafkaHosts.get)
      .option("subscribePattern", appConfig.input.get )
      .option("startingOffsets", appConfig.startingOffsets.get)
      .load().as[KafkaMessage]

    def readCsvRecords(header: String,content: String, delimiter: Char = ',') = {
      CSVFormat.RFC4180.withIgnoreSurroundingSpaces
        .withIgnoreEmptyLines
        .withTrim
        .withHeader(header.split(delimiter): _*)
        .withDelimiter(delimiter)
        .parse(new StringReader(content))
    }

    /**
     * sender:	string -	raw sender phone number
     * recipient:	string -	raw recipient phone number
     * sender_norm:	string -	normalised sender phone number; remove all non-digit
     * recipient_norm:	string -	normalised recipient phone number; remove all non-digit
     * sms_timestamp:	timestamp -	timestamp of when the sms is sent
     * processed_timestamp:	timestamp -	timestamp of when the etl is processing
     * country:	string -	short country code
     * country_name:	string -	full country name
     * latitude:	double -	latitude of the country
     * longitude:	double	- longitude of the country
     * content:	string -	raw content
     */

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
        countryName = "", // TODO, populate this field with reference dataset
        latitude = 0, // TODO, populate this field with reference dataset
        longitude = 0, // TODO, populate this field with reference dataset
        isSpam = false // TODO, populate this field with reference dataset
      )
    }).map(record=> (record.toByteArray, appConfig.output.get))
      .toDF("value","topic")


    resultDf.writeStream.format("kafka")
            .trigger(Trigger.ProcessingTime(30 * 1000L))
            .outputMode(OutputMode.Append())
            .option("checkpointLocation", appConfig.checkpoint.get)
            .start()
            .awaitTermination()

  }
}

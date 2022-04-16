package app

import java.time.Instant

import com.google.protobuf.timestamp.Timestamp
import common.BaseApp
import scalapb.protos.kafka.Sms


/**
 * Implement an ETL that reads CSV SMS records from a directory, enrich it and write the result into a directory
 */

object BatchSmsEtl extends BaseApp {

  def process(): Unit = {

    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header","true")
      .load(appConfig.input.get)

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

    val smsDs = df.map(r=>{
        Sms(
          sender = r.getAs[String]("sender"),
          recipient = r.getAs[String]("recipient"),
          senderNorm = r.getAs[String]("sender").filter(_.isDigit),
          recipientNorm =  r.getAs[String]("recipient").filter(_.isDigit),
          smsTimestamp = Some(Timestamp(r.getAs[String]("timestamp").toLong)),
          processedTimestamp = Some(Timestamp(Instant.now.getEpochSecond)),
          country = r.getAs[String]("country"),
          content = r.getAs[String]("content"),
          countryName = "", // TODO, populate this field with reference dataset
          latitude = 0, // TODO, populate this field with reference dataset
          longitude = 0, // TODO, populate this field with reference dataset
          isSpam = false // TODO, populate this field with reference dataset
        )
      })

    smsDs.coalesce(2)
      .write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save(appConfig.output.get)

  }
}

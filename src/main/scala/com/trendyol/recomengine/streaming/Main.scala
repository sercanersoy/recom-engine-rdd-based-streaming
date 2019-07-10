package com.trendyol.recomengine.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("recom-engine-streaming")
      .getOrCreate()

    val model: MatrixFactorizationModel = MatrixFactorizationModel.load(spark.sparkContext, "/models/model")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka1:19092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "reviewStream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("reviews")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
      .map(record => record.value)
      .map(record => {
        val fields = record.split(",")
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
      })

    stream.foreachRDD(rdd => {
      rdd.collect().foreach(row => {
        print("recommendation list for user " + row.user + ": ")
        val recommendationList = Try(model.recommendProducts(row.user, 10)).getOrElse(null)
        if (recommendationList != null) {
          recommendationList.foreach(rating => {
            print(rating.product + " ")
          })
        } else {
          print("no recommendation for the user.")
        }
        println()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

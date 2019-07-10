package com.trendyol.recomengine.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("recom-engine-streaming")
      .master("local")
//      .master("spark://spark-master:7077")
      .getOrCreate()

    val model : MatrixFactorizationModel = MatrixFactorizationModel.load(spark.sparkContext, "/home/yasin.uygun@trendyol.work/workspace/java/recom-engine-ml/model")

    import spark.implicits._

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
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

    stream.map(record => {
//      Try((record, model.recommendProducts(record.user, 10))).getOrElse((record, null))
      (record, model.recommendProducts(record.user, 10))
    }).print

    ssc.start()
    ssc.awaitTermination()
  }
}

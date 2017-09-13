package com.the_ica.stream.spark

import java.lang

import com.datastax.spark.connector.cql.CassandraConnector
import com.the_ica.stream.avro.AvroSerde
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010._


import scala.collection.JavaConverters._
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.typesafe.config.ConfigFactory
import com.datastax.spark.connector.streaming._
import org.apache.spark.{SparkConf, SparkContext}


object DStreamJob {

  case class Rec(sc: Long, date: Int, mtms: Array[Double])

  def write2cassandra(sc: Long, mtms: Iterable[Array[lang.Double]]): Unit = {
    val sz = mtms
    println("flushing to C*")
  }

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load

    val sc = new SparkContext(new SparkConf(true)
      .setMaster(conf.getString("spark.master")).setAppName("Streamer")
      .set("spark.cassandra.connection.host", "localhost")
     // .set("spark.cassandra.auth.username", "cassandra")
     //  .set("spark.cassandra.auth.password", "cassandra")
    )

    val streamingContext = new StreamingContext(sc, Seconds(conf.getInt("spark.stream.batchInterval")))

    val connector = CassandraConnector(sc.getConf)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("kafka.brokers"),
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "mtms-rdd",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics: Array[String] = conf.getString("kafka.topics").split(",")

/*
    val stream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]] = ???
     /* KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(Array("mtms-trades"), kafkaParams)
      )*/

    val rd = stream.foreachRDD {
      (rdd, time: Time) =>
        if (!rdd.isEmpty()) {
          val count = rdd.count
          println("Received in this RDD " + count)

          val grouped = rdd.map { data => (AvroSerde.mtmKeyDeserialize(data.key()), AvroSerde.mtmValueDeserialize(data.value())) }
            .map(x => (x._1.getScenario, x._2.getMtms.asScala.toArray)) // Keep the date??
            .groupByKey() //TODO Remove this ==> reduceByKey
            .foreach{
                case (sc, mtms) =>{
                  connector.withSessionDo( session => session.execute("INSERT blablbala"))

                } //write2cassandra(sc, mtms)
             }
        }
    }*/
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

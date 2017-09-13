package com.the_ica.stream.kafka


import com.the_ica.stream.avro.AvroSerde
import com.typesafe.config.ConfigFactory
import org.apache.kafka.streams.{KafkaStreams, KeyValue}
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder

import scala.language.implicitConversions

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object KeyValueImplicits {

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

}


object KaStream {


  val config = ConfigFactory.load()
  import org.apache.kafka.common.serialization.Serdes
  import org.apache.kafka.streams.StreamsConfig

  val streamsConfiguration = new java.util.Properties()
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "mtms-aggregator")
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.ByteArray().getClass.getName)
  streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass.getName)



  val builder  = new KStreamBuilder

  val stream = builder.stream[Array[Byte], Array[Byte]] (config.getString("kafka.topics"))

  import  KeyValueImplicits._
/*

  stream.map((key : Array[Byte], value : Array[Byte]) =>
    (AvroSerde.mtmKeyDeserialize(key), AvroSerde.mtmValueDeserialize(value))
  )
*/




}

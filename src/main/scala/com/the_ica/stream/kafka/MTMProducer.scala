package com.the_ica.stream.kafka

import java.util._

import com.the_ica.applications.commons.messages.avro.{MtmTradeMessageKey, MtmTradeMessageValue}
import com.the_ica.stream.avro.AvroSerde
import com.typesafe.config.ConfigFactory

import scala.util.Random
import scala.util.control.NonFatal

object MTMProducer {
  import org.apache.kafka.clients.producer._

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load

    val conf = new java.util.Properties()

    conf.put("bootstrap.servers", "localhost:9092" ) //config.getString("kafka.brokers"))
    conf.put("acks", "1")
    conf.put("request.timeout.ms", "30000")
    conf.put("retries", "2")
    conf.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    conf.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    // conf.put("schema.registry.url", "http://localhost:8081")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](conf)

    val nbRecords = config.getInt("kafka.nbRecords")
    for (i <- 1 to nbRecords) {
      val message = randomMessage

      val key = AvroSerde.mtmKeySerialize(message._1)

      message._2.foreach { v =>
        val record = new ProducerRecord(config.getString("kafka.topics"), key, AvroSerde.mtmValueSerialize(v))

        try {
          producer.send(record, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception != null) {
                exception.printStackTrace()
              }
              println("Pushed ==> " + record)
            }
          })
        } catch {
          case NonFatal(e) => println("Error !!!!!!!! " + e.getMessage)
        }
      }
    }
    producer.close
  }

  def randomMessage: (MtmTradeMessageKey, IndexedSeq[MtmTradeMessageValue]) = {
    val key = MtmTradeMessageKey.newBuilder()
      .setJobId(UUID.randomUUID().toString)
      .setScenario(new Random().nextLong())
      .setHopLimit(1)
      .build()

    val values = (1 to 76).map(
      MtmTradeMessageValue.newBuilder()
        .setDate(_)
        .setMtms(Arrays.asList(Random.nextDouble(), Random.nextDouble()))
        .build()
    )

    (key, values)
  }

}


package com.the_ica.stream.spark

import com.datastax.spark.connector.cql.CassandraConnector
import com.the_ica.applications.commons.messages.avro.{MtmTradeMessageKey, MtmTradeMessageValue}
import com.the_ica.stream.avro.AvroSerde
import com.the_ica.stream.utils.CassandraUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}
import scala.concurrent.duration._

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import org.apache.spark.sql.streaming.{ProcessingTime => DeprecatedProcessingTime, _}
import org.apache.spark.sql.streaming.Trigger

import scala.collection.JavaConverters._

object MtmEncoders {
  implicit def mtmValuesEncoder: org.apache.spark.sql.Encoder[MtmTradeMessageValue] =
    org.apache.spark.sql.Encoders.javaSerialization(classOf[MtmTradeMessageValue])

  implicit def mtmKeysEncoder: org.apache.spark.sql.Encoder[MtmTradeMessageKey] =
    org.apache.spark.sql.Encoders.javaSerialization(classOf[MtmTradeMessageKey])
}

object StreamingJob {

  case class Rec(scenario: Long, date: Int, mtms: Array[Double])

  def main(args: Array[String]) {

    val config = ConfigFactory.load

    // Get Spark session
    val session =
      SparkSession.builder
        .master(config.getString("spark.master"))
        .appName("Kafka Streaming Aggreg")
        .config("spark.cassandra.connection.host", "localhost") //config.getString("cassandra.seeds"))
        .getOrCreate()

    import session.implicits._

    /*
    val connector = CassandraConnector.apply(session.sparkContext.getConf)
    // Create keyspace and tables here, NOT in prod
    connector.withSessionDo { session =>
      CassandraUtils.createKeySpaceAndTable(session, true)
    }
*/

    //Open a connection to Kafka
    val tuples = session.readStream
      .format("kafka")
      .option("subscribe", config.getString("kafka.topics"))
      .option("kafka.bootstrap.servers", config.getString("kafka.brokers"))
      .option("startingOffsets", "latest")
      .load()

    tuples.printSchema()

   val df1= tuples.selectExpr(
        "CAST (key AS BINARY)",
        "CAST (value as BINARY)"
      )
      .as[(Array[Byte], Array[Byte])]

     df1.printSchema


     val df =  df1.map { kvs =>
        val kv: (MtmTradeMessageKey, MtmTradeMessageValue) = (AvroSerde.mtmKeyDeserialize(kvs._1), AvroSerde.mtmValueDeserialize(kvs._2))
        (kv._1.getScenario, kv._2.getDate, kv._2.getMtms.asScala.toArray)
      }
      .toDF(List("scenario", "date", "mtms"): _*)
      .as[Rec]

    df.printSchema()

    val ds = df.groupByKey(_.scenario)
      .mapGroups {
        // TODO Compute Trajectory

        case (sc: Long, recs: Iterator[Rec]) => (sc, recs.map(_.mtms.mkString(" ")).toList) //TODO converter Array[MTMTS] => ByteBuffer
      }.toDF("scenario", "mtms")

    ds.printSchema()

   // ds.show()
/*

  val query= ds
      .writeStream
      .queryName("Parquet Outputter")
      .format("parquet")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/spark")
      .trigger(ProcessingTime(2.seconds))
      .start("/Users/samklr/code/ica/parquet2")

    query.awaitTermination
*/



   val query = ds.writeStream.queryName("Stream Aggreg").foreach(simpleWriter).start()
    query.awaitTermination

    session.close
  }

  def buildTrajectory(rec: Rec) =  ???


  //  Foreach sink writer to push the output to cassandra.

  def simpleWriter = new ForeachWriter[Row] {
    override def open(partitionId: Long, version: Long) = true

    override def process(row: Row) = {
      val sc = row.getLong(0)
      val mtmt = row.getAs[Seq[String]]("mtms")

      println("Processing Scenario : " + sc + " With " + mtmt.length + " Mtms")
    }

    override def close(errorOrNull: Throwable) = { // TODO Extra Logic to close this
      println("Closing Foreach Writer ")
    }

  }

  def cassandraWriter(connector: CassandraConnector) = new ForeachWriter[Row] {
    override def open(partitionId: Long, version: Long) = true

    override def process(row: Row) = writeRowtoCassandra(row, connector)

    override def close(errorOrNull: Throwable) = { // TODO Extra Logic to close this
      println("Closing Foreach Writer ")
    }
  }

  def writeRowtoCassandra(row: Row, connector: CassandraConnector) = {
    val sc = row.getLong(0)
    val mtmt = row.getAs[Seq[String]]("mtms")

    //mtmt.foreach(println)
    connector.withSessionDo { session =>
      CassandraUtils.bytesStmt(session, sc, mtmt)
    }
    println(" => Wrote  to Cassandra " + mtmt.length + " mtms")
  }

}

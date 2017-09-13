package com.the_ica.stream.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import java.util.Properties


object MySQLFetcher extends App{

  val config = ConfigFactory.load

  // Get Spark session
  val spark =
    SparkSession.builder
      .master(config.getString("spark.master"))
      .appName("MySQL Fetch")
      .getOrCreate()


  import spark.implicits._


  // Option 1: Build the parameters into a JDBC url to pass into the DataFrame APIs
  val jdbcUsername = "USER_NAME"
  val jdbcPassword = "PASSWORD"
  val jdbcHostname = "HOSTNAME"
  val jdbcPort = 3306
  val jdbcDatabase ="DATABASE"
  val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"

  // Option 2: Create a Properties() object to hold the parameters. You can create the JDBC URL without passing in the user/password parameters directly.
  val connectionProperties = new Properties()
  connectionProperties.put("user", "USER_NAME")
  connectionProperties.put("password", "PASSWORD")

  val jdbc_url = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
  val tableDF = spark.read.jdbc(jdbc_url, "employees", connectionProperties)

  tableDF.show


}

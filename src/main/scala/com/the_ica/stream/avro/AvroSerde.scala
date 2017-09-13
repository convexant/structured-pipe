package com.the_ica.stream.avro

import java.io.{ByteArrayInputStream, ObjectInputStream}

import com.the_ica.applications.commons.messages.avro.{MtmTradeMessageKey, MtmTradeMessageValue}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs

object AvroSerde {

  implicit private val specificMtmAvroBinaryInjection: Injection[MtmTradeMessageValue, Array[Byte]] =
    SpecificAvroCodecs.toBinary[MtmTradeMessageValue]

  implicit private val specificMtmKeysAvroBinaryInjection: Injection[MtmTradeMessageKey, Array[Byte]] =
    SpecificAvroCodecs.toBinary[MtmTradeMessageKey]

  def mtmValueDeserialize(bytes: Array[Byte]): MtmTradeMessageValue = {
    val attempt = Injection.invert[MtmTradeMessageValue, Array[Byte]](bytes)
    attempt.get
  }

  def mtmKeyDeserialize(bytes: Array[Byte]): MtmTradeMessageKey = {
    val attempt = Injection.invert[MtmTradeMessageKey, Array[Byte]](bytes)
    attempt.get
  }

  def mtmValueSerialize(mtmTradeMessageValue: MtmTradeMessageValue): Array[Byte] = specificMtmAvroBinaryInjection(mtmTradeMessageValue)

  def mtmKeySerialize(mtmTradeMessageKey: MtmTradeMessageKey): Array[Byte] = specificMtmKeysAvroBinaryInjection(mtmTradeMessageKey)

  def genericAvroDeserializer[A <: Serializable] = (bytes: Array[Byte]) => {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    val a = ois.readObject.asInstanceOf[A]
    ois.close
    a
  }
}

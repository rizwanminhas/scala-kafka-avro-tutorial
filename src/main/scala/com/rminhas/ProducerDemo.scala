package com.rminhas

import com.rminhas.generated.v1.{MySchema => MySchemaV1}
import com.rminhas.generated.v2.{MySchema => MySchemaV2}
import com.rminhas.generated.v3.{MySchema => MySchemaV3}
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties

object ProducerDemo extends App {
  val logger = LoggerFactory.getLogger(ProducerDemo.getClass)

  //props.put(ProducerConfig.ACKS_CONFIG, "all")
  //props.put(ProducerConfig.RETRIES_CONFIG, 0)

  val TOPIC_NAME = "rizwan.test.topic"

  produceV1SchemaMessage()
  produceV2SchemaMessage()
  produceV3SchemaMessage()
  produceNoSchemaMessage()

  def myCallBack[A, B](record: ProducerRecord[A, B]): Callback =
    (metadata: RecordMetadata, exception: Exception) =>
      if (exception == null)
        logger.info(s"""
                       |\n\n${"*".repeat(20)}
                       |Message: ${record.key()} -> ${record.value()}
                       |Topic: ${metadata.topic()}
                       |Partition: ${metadata.partition()}
                       |Offset: ${metadata.offset()}
                       |Timestamp: ${metadata.timestamp()}
                       |${"*".repeat(20)}\n\n
            """.stripMargin)
      else
        logger.error(s"\n\n${"*".repeat(20)}\nError: $exception\n${"*".repeat(20)}\n\n")

  def propsWithSchema(): Properties = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
    props
  }

  def propsWithoutSchema(): Properties = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
    props
  }

  def produceV1SchemaMessage(): Unit = {
    val producer = new KafkaProducer[String, MySchemaV1](propsWithSchema())
    val data     = new MySchemaV1("rizwanV1", 1)
    val record   = new ProducerRecord[String, MySchemaV1](TOPIC_NAME, data.getName, data)
    producer.send(record, myCallBack(record))
    producer.flush()
    producer.close()
  }

  def produceV2SchemaMessage(): Unit = {
    val producer = new KafkaProducer[String, MySchemaV2](propsWithSchema())
    val data     = new MySchemaV2("rizwanV2", "minhasV2", 2)
    val record   = new ProducerRecord[String, MySchemaV2](TOPIC_NAME, data.getFirstName, data)
    producer.send(record, myCallBack(record))
    producer.flush()
    producer.close()
  }

  def produceV3SchemaMessage(): Unit = {
    val producer = new KafkaProducer[String, MySchemaV3](propsWithSchema())
    val data     = new MySchemaV3("rizwanV3", "minhasV3", 3, "rizwan@test.com")
    val record   = new ProducerRecord[String, MySchemaV3](TOPIC_NAME, data.getFirstName, data)
    producer.send(record, myCallBack(record))
    producer.flush()
    producer.close()
  }

  def produceNoSchemaMessage(): Unit = {
    val producer = new KafkaProducer[String, String](propsWithoutSchema())
    val record   = new ProducerRecord[String, String](TOPIC_NAME, "test_key", "test_data")
    producer.send(record, myCallBack(record))
    producer.flush()
    producer.close()
  }
}

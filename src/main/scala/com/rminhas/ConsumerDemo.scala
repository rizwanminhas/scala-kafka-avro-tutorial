package com.rminhas

import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroDeserializerConfig}
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer
import com.rminhas.generated.v1.{MySchema => MySchemaV1}
import com.rminhas.generated.v2.{MySchema => MySchemaV2}
import com.rminhas.generated.v3.{MySchema => MySchemaV3}

import java.time.Duration.ofMillis
import java.util.Properties

object ConsumerDemo extends App {

  val TOPIC_NAME = "rizwan.test.topic"

  val v1SchemaConsumer = createConsumerWithSchema[MySchemaV1]("consumer-groupv1")
  val v2SchemaConsumer = createConsumerWithSchema[MySchemaV2]("consumer-groupv2")
  val v3SchemaConsumer = createConsumerWithSchema[MySchemaV3]("consumer-groupv3")
  val noSchemaConsumer = createConsumerWithoutSchema("consumer-group4")

  while (true) {
    printRecords(v1SchemaConsumer, "V1 Consumer:")
    printRecords(v2SchemaConsumer, "V2 Consumer:")
    printRecords(v3SchemaConsumer, "V3 Consumer:")
    printRecords(noSchemaConsumer, "No Schema Consumer:")
  }

  def getCommonProps: Properties = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString.toLowerCase)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") // This will make it read the message on next run.
    props.put("schema.registry.url", "http://localhost:8081")
    props
  }

  def createPropsWithSchema(consumerGroupName: String): Properties = {
    val props = getCommonProps
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName)
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
    props
  }

  def createPropsWithoutSchema(consumerGroupName: String): Properties = {
    val props = getCommonProps
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName)
    props
  }

  def createConsumerWithSchema[V](groupName: String): KafkaConsumer[String, V] = {
    val consumer = new KafkaConsumer[String, V](createPropsWithSchema(groupName))
    consumer.subscribe(java.util.Arrays.asList(TOPIC_NAME))
    consumer
  }

  def createConsumerWithoutSchema(groupName: String): KafkaConsumer[String, String] = {
    val consumer = new KafkaConsumer[String, String](createPropsWithoutSchema(groupName))
    consumer.subscribe(java.util.Arrays.asList(TOPIC_NAME))
    consumer
  }

  def printRecords[V](consumer: KafkaConsumer[String, V], prefix: String): Unit =
    consumer
      .poll(ofMillis(50))
      .forEach(data => {
        data.value() match {
          case message: MySchemaV1 => println(s"$prefix name=${message.getName},age=${message.getAge}")
          case message: MySchemaV2 =>
            println(s"$prefix firstname=${message.getFirstName},lastname=${message.getLastName},age=${message.getAge}")
          case message: MySchemaV3 =>
            println(
              s"$prefix firstname=${message.getFirstName},lastname=${message.getLastName},age=${message.getAge},email=${message.getEmail}"
            )
          case _ => println(s"$prefix Unknown schema. Message value = ${data.value()}")
        }
      })
}
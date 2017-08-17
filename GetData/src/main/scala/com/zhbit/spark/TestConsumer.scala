package com.zhbit.spark

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

/**
  * Created by admin on 2017/8/11.
  *
  * start producer
  * ./bin/kafka-console-producer.sh --broker-list datanode1:9092,datanode2:9092,datanode3:9092 --topic test1
  *
  * start consumer
  * ./bin/kafka-console-consumer.sh --bootstrap-server datanode1:9092 --zookeeper datanode1:2181,datanode2:2181,datanode3:2181 --topic test1 --from-beginning
  */
object TestConsumer {

  def main(args: Array[String]) {

    val props: Properties = new Properties()

    props.put("bootstrap.servers", "datanode1:9092,datanode2:9092,datanode3:9092")

    props.put("group.id", "test1")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    val list = new util.ArrayList[String]

    list.add("test")

    consumer.subscribe(list)

    var i = 0

    while (true) {

      val records: ConsumerRecords[String, String] = consumer.poll(100)

      val it = records.iterator()
      while (it.hasNext){
        println(it.next().value())
      }

      Thread sleep 3000

    }

  }


}

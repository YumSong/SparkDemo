package com.zhbit.demo.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by admin on 2017/8/11.
  *
  * start producer
  * ./bin/kafka-console-producer.sh --broker-list datanode1:9092,datanode2:9092,datanode3:9092 --topic test1
  *
  * start consumer
  * ./bin/kafka-console-consumer.sh --bootstrap-server datanode1:9092 --zookeeper datanode1:2181,datanode2:2181,datanode3:2181 --topic test1 --from-beginning
  */
object TestProductor {

  def main(args: Array[String]) {
    val props:Properties = new Properties()

    props.put("bootstrap.servers", "datanode1:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer:KafkaProducer[String,String] = new KafkaProducer[String,String] (props)

    var i = 100

    while (i>0){

      println("producer sends message: " + i)

      producer.send(new ProducerRecord[String,String]("test", i + "", i + ""))

      i = i-1
    }
    producer.close()
  }


}

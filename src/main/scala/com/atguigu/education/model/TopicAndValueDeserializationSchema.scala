package com.atguigu.education.model

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicAndValueDeserializationSchema extends KafkaDeserializationSchema[TopicAndValue] {
  //表示是不是流最后一条元素，实时的是无界数据
  override def isEndOfStream(t: TopicAndValue): Boolean = {
    false
  }

  // 消费者消费kafka的时候，顺便指定从kafka提取出来的反序列化方式，kafka存储的是二进制数据
  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): TopicAndValue = {
    new TopicAndValue(consumerRecord.topic(), new String(consumerRecord.value(), "utf-8"))
  }

  //告诉flink 数据类型
  override def getProducedType: TypeInformation[TopicAndValue] = {
    TypeInformation.of(new TypeHint[TopicAndValue] {})
  }
}

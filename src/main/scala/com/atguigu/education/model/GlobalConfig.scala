package com.atguigu.education.model

object GlobalConfig {
  val HBASE_ZOOKEEPER_QUORUM = "hadoop101,hadoop102,hadoop103"
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

  val BOOTSTRAP_SERVERS = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
  val ACKS = "-1"

  val KUDU_MASTER = "hadoop101"
  val KUDU_TABLE_DWDBASEAD = "impala::education.dwd_base_ad"
  val KUDU_TABLE_DWDBASEWEBSITE = "impala::education.dwd_base_website"
  val KUDU_TABLE_DWDVIPLEVEL = "impala::education.dwd_vip_level"
  val KUDU_TABLE_DWSMEMBER = "impala::education.dws_member"
}

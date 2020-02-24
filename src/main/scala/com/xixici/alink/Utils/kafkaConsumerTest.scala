package com.xixici.alink.Utils

import com.alibaba.alink.operator.stream.StreamOperator
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp
import com.alibaba.alink.operator.stream.source.Kafka011SourceStreamOp

/**
  * Created by yang.lei01 on 2020/2/24.
  */
object kafkaConsumerTest {
  def main(args: Array[String]): Unit = {
    val schemaArray: Array[String] =
      Array(
        "$.id",
        "$.click",
        "$.dt",
        "$.C1",
        "$.banner_pos",
        "$.site_id",
        "$.site_domain",
        "$.site_category",
        "$.app_id",
        "$.app_domain",
        "$.app_category",
        "$.device_id",
        "$.device_ip",
        "$.device_model",
        "$.device_type",
        "$.device_conn_type",
        "$.C14",
        "$.C15",
        "$.C16",
        "$.C17",
        "$.C18",
        "$.C19",
        "$.C20",
        "$.C21"
      )
    val source: Kafka011SourceStreamOp = new Kafka011SourceStreamOp()
      .setBootstrapServers("127.0.0.1:9092")
      .setTopic("avazu")
      .setStartupMode("GROUP_OFFSETS")
      .setGroupId("alink_group")
    val data = source
      .link(
        new JsonValueStreamOp()
          .setSelectedCol("message")
          .setReservedCols("")
          .setOutputCols(
            "id",
            "click",
            "dt",
            "C1",
            "banner_pos",
            "site_id",
            "site_domain",
            "site_category",
            "app_id",
            "app_domain",
            "app_category",
            "device_id",
            "device_ip",
            "device_model",
            "device_type",
            "device_conn_type",
            "C14",
            "C15",
            "C16",
            "C17",
            "C18",
            "C19",
            "C20",
            "C21"
          )
          .setJsonPath(schemaArray)
          .setSkipFailed(true)
      )
      .select(
        "id, "
          + "click, "
          + "dt, "
          + "C1, "
          + "CAST(banner_pos AS int) AS banner_po, "
          + "site_id, "
          + "site_domain, "
          + "site_category, "
          + "app_id, "
          + "app_domain, "
          + "app_category, "
          + "device_id, "
          + "device_ip, "
          + "device_model, "
          + "device_type, "
          + "device_conn_type, "
          + "CAST(C14 AS int) AS C14, "
          + "CAST(C15 AS int) AS C15, "
          + "CAST(C16 AS int) AS C16, "
          + "CAST(C17 AS int) AS C17, "
          + "CAST(C18 AS int) AS C18, "
          + "CAST(C19 AS int) AS C19, "
          + "CAST(C20 AS int) AS C20, "
          + "CAST(C21 AS int) AS C21"
      )
    println(data.getSchema)
    data.print(10, 20)
    StreamOperator.execute
  }
}

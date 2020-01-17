package com.xixici.alink.AssociationRule

import com.alibaba.alink.operator.batch.associationrule.{FpGrowthBatchOp, PrefixSpanBatchOp}
import com.alibaba.alink.operator.batch.source._

/**
  * Created by yang.lei01 on 2020/1/16.
  */
object PrefixSpanBatchOp {
  def main(args: Array[String]): Unit = {
    val URL = "data/PrefixSpan-test-demo.csv"
    val SCHEMA_STR = "items string"
    val data = new CsvSourceBatchOp()
      .setFilePath(URL)
      .setSchemaStr(SCHEMA_STR)
    val prefixSpanBatchOp = new PrefixSpanBatchOp()
      .setItemsCol("items")
    .setMinSupportCount(3)
    val output = prefixSpanBatchOp.linkFrom(data)
    output.print()
    prefixSpanBatchOp.getSideOutput(0).print()
  }
}

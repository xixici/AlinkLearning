package com.xixici.alink.AssociationRule

import com.alibaba.alink.operator.batch.associationrule.FpGrowthBatchOp
import com.alibaba.alink.operator.batch.source._
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler

/**
  * Created by yang.lei01 on 2020/1/16.
  */
object FpGrowthBatchOp {
  def main(args: Array[String]): Unit = {
    val URL = "data/FpGrowth-test-demo.csv"
    val SCHEMA_STR = "items string"
    val data = new CsvSourceBatchOp()
      .setFilePath(URL)
      .setSchemaStr(SCHEMA_STR)
    val fpGrowthBatchOp = new FpGrowthBatchOp()
      .setItemsCol("items")
      .setMinSupportPercent(0.4)
      .setMinConfidence(0.6)
    val output = fpGrowthBatchOp.linkFrom(data)
    output.print()
    output.getSideOutput(0).print()
  }
}

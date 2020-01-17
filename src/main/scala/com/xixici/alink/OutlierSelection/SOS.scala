package com.xixici.alink.Clustering

import com.alibaba.alink.operator.batch.outlier.SosBatchOp
import com.alibaba.alink.operator.batch.source._
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler

/**
  * Created by yang.lei01 on 2020/1/16.
  */
object SOS {
  def main(args: Array[String]): Unit = {
    val URL = "data/Mall_Costumers.csv"
    val SCHEMA_STR =
      "CustomerID int, Age int, Annual_Income int, Spending_Score int"
    val data = new CsvSourceBatchOp()
      .setFilePath(URL)
      .setSchemaStr(SCHEMA_STR)
      .setIgnoreFirstLine(true)

    val assembler = new VectorAssembler()
      .setSelectedCols("CustomerID", "Age", "Annual_Income", "Spending_Score")
      .setOutputCol("vec")
    val assemblerData = assembler.transform(data)

    val sosBatchOp = new SosBatchOp()
      .setVectorCol("vec")
      .setPredictionCol("outlier_score")
      .setPerplexity(3.0)
    val output =
      sosBatchOp.linkFrom(assemblerData.select(Array("vec")))
    output.firstN(10).print()
  }
}

package com.xixici.alink.Clustering

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp
import com.alibaba.alink.operator.batch.evaluation.{
  EvalClusterBatchOp,
  EvalRegressionBatchOp
}
import com.alibaba.alink.operator.batch.source._
import com.alibaba.alink.pipeline.Pipeline
import com.alibaba.alink.pipeline.clustering.BisectingKMeans
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler
import com.alibaba.alink.pipeline.regression._

/**
  * Created by yang.lei01 on 2020/1/16.
  */
object BisectingKMeans {
  def main(args: Array[String]): Unit = {
    val URL = "data/Mall_Costumers.csv"
    val SCHEMA_STR =
      "CustomerID int, Age int, Annual_Income int, Spending_Score int"
    val data = new CsvSourceBatchOp()
      .setFilePath(URL)
      .setSchemaStr(SCHEMA_STR)
      .setIgnoreFirstLine(true)

    val spliter = new SplitBatchOp().setFraction(0.8)
    spliter.linkFrom(data)

    val trainData = spliter
    val testData = spliter.getSideOutput(0)

    val assembler = new VectorAssembler()
      .setSelectedCols("CustomerID", "Age", "Annual_Income", "Spending_Score")
      .setOutputCol("vec")
    val pipeline = new Pipeline().add(assembler)

    val bisectingKMeans = new BisectingKMeans()
      .setVectorCol("vec")
      .setK(2)
      .setPredictionCol("pred")

    val model = pipeline.add(bisectingKMeans).fit(trainData)
    val predictBatch = model.transform(testData)
    val metrics = new EvalClusterBatchOp()
      .setVectorCol("vec")
      .setPredictionCol("pred")
      .linkFrom(predictBatch)
      .collectMetrics()

    val ch = metrics.getCalinskiHarabaz.formatted("%.2f")
    model.save(f"model/BisectingKMeans-$ch(CH).csv")
    BatchOperator.execute()

    println("Total Samples Number:", metrics.getCount())
    println("Cluster Number:", metrics.getK())
    println("Cluster Array:", metrics.getClusterArray().mkString(","))
    println("Cluster Count Array:", metrics.getCountArray().mkString(","))
    println("CP:", metrics.getCompactness())
    println("DB:", metrics.getDaviesBouldin())
    println("SP:", metrics.getSeperation())
    println("SSB:", metrics.getSsb())
    println("SSW:", metrics.getSsw())
    println("CH:", metrics.getCalinskiHarabaz())

  }
}

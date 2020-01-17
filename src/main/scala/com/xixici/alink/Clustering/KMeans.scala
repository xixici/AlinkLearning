package com.xixici.alink.Clustering

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp
import com.alibaba.alink.operator.batch.source._
import com.alibaba.alink.pipeline.Pipeline
import com.alibaba.alink.pipeline.clustering.{BisectingKMeans, KMeans}
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler

/**
  * Created by yang.lei01 on 2020/1/16.
  */
object KMeans {
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

    val kMeans = new KMeans()
      .setVectorCol("vec")
      .setK(2)
      .setPredictionCol("pred")

    val model = pipeline.add(kMeans).fit(trainData)
    val predictBatch = model.transform(testData)
    val metrics = new EvalClusterBatchOp()
      .setVectorCol("vec")
      .setPredictionCol("pred")
      .linkFrom(predictBatch)
      .collectMetrics()

    val ch = metrics.getCalinskiHarabaz.formatted("%.2f")
    model.save(f"model/KMeans-$ch(CH).csv")
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

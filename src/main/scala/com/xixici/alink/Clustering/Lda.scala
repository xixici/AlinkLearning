package com.xixici.alink.Clustering

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp
import com.alibaba.alink.operator.batch.source._
import com.alibaba.alink.pipeline.Pipeline
import com.alibaba.alink.pipeline.clustering.{KMeans, Lda}
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler

/**
  * Created by yang.lei01 on 2020/1/16.
  */
object Lda {
  def main(args: Array[String]): Unit = {
    val URL = "data/lda-test-demo.csv"
    val SCHEMA_STR = "doc string"
    val data = new CsvSourceBatchOp()
      .setFilePath(URL)
      .setSchemaStr(SCHEMA_STR)
      .setIgnoreFirstLine(true)

    val spliter = new SplitBatchOp().setFraction(0.8)
    spliter.linkFrom(data)

    val trainData = spliter
    val testData = spliter.getSideOutput(0)

    val pipeline = new Pipeline()

    val lda = new Lda()
      .setSelectedCol("doc")
      .setTopicNum(5)
      .setMethod("online")
      .setSubsamplingRate(1.0)
      .setPredictionCol("pred")

    val model = pipeline.add(lda).fit(trainData)
    val predictBatch = model.transform(testData)
    val metrics = new EvalClusterBatchOp()
      .setPredictionCol("pred")
      .linkFrom(predictBatch)
      .collectMetrics()

    val ch = metrics.getCalinskiHarabaz.formatted("%.2f")
    model.save(f"model/Lda-$ch(CH).csv")
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

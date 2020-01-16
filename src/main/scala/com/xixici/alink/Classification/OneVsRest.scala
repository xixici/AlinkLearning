package com.xixici.alink.Classification

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp
import com.alibaba.alink.operator.batch.evaluation._
import com.alibaba.alink.operator.batch.source._
import com.alibaba.alink.pipeline.Pipeline
import com.alibaba.alink.pipeline.classification._
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler

/**
  * Created by yang.lei01 on 2020/1/16.
  */
object OneVsRest {
  def main(args: Array[String]): Unit = {
    val URL = "data/iris.csv"
    val SCHEMA_STR =
      "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
    val data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

    val spliter = new SplitBatchOp().setFraction(0.8)
    spliter.linkFrom(data)

    val trainData = spliter
    val testData = spliter.getSideOutput(0)

    val assembler = new VectorAssembler()
      .setSelectedCols("sepal_length",
                       "sepal_width",
                       "petal_length",
                       "petal_width")
      .setOutputCol("vec")
      .setReservedCols("category")
    val pipeline = new Pipeline().add(assembler)

    val logisticRegression =
      new LogisticRegression()
        .setVectorCol("vec")
        .setLabelCol("category")
        .setMaxIter(100)
        .setPredictionCol("pred_label")
        .setPredictionDetailCol("pred_detail")
        .setReservedCols("category")
    val oneVsRest =
      new OneVsRest().setClassifier(logisticRegression).setNumClass(3)
    val model = pipeline.add(oneVsRest).fit(trainData)
    val predictBatch = model.transform(testData)
    predictBatch.firstN(10).print()
    val metrics = new EvalMultiClassBatchOp()
      .setLabelCol("category")
      .setPredictionDetailCol("pred_detail")
      .linkFrom(predictBatch.select(Array("category", "pred_detail")))
      .collectMetrics()
    val acc = (metrics.getAccuracy() * 100).formatted("%.2f")
    model.save(f"model/oneVsRest-$acc%%(Acc).csv")
    BatchOperator.execute()
    println("Accuracy:", metrics.getAccuracy())
    println("Macro Precision:", metrics.getMacroPrecision())
    println("Micro Recall:", metrics.getMicroRecall())
    println("Weighted Sensitivity:", metrics.getWeightedSensitivity())
  }
}

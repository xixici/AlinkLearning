package com.xixici.alink.Regression

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp
import com.alibaba.alink.operator.batch.source._
import com.alibaba.alink.pipeline.regression._
import com.alibaba.alink.pipeline.{Pipeline, PipelineModel}

/**
  * Created by yang.lei01 on 2020/1/16.
  */
object AftSurvivalRegression {

  /**
    * description data is not satisfied. This is not success.
    * param
    *
    * @return
    * @author Yang.Lei
    */
  def main(args: Array[String]): Unit = {
    val URL = "data/50_startups.csv"
    val SCHEMA_STR =
      "R_D_Spend double, Administration double, Marketing_Spend double, Profit double"
    val data = new CsvSourceBatchOp()
      .setFilePath(URL)
      .setSchemaStr(SCHEMA_STR)
      .setIgnoreFirstLine(true)

    val spliter = new SplitBatchOp().setFraction(0.8)
    spliter.linkFrom(data)

    val trainData = spliter
    val testData: BatchOperator[_] = spliter.getSideOutput(0)

    val aftSurvivalRegression = new AftSurvivalRegression()
      .setFeatureCols(
        "R_D_Spend",
        "Administration",
        "Marketing_Spend"
      )
      .setLabelCol("Profit")
      .setPredictionCol("pred")
      .setCensorCol("Administration")
    val pipeline = new Pipeline()

    val model: PipelineModel = pipeline.add(aftSurvivalRegression).fit(trainData)
    val predictBatch: BatchOperator[_ <: BatchOperator[_]] = model.transform(testData)
    val metrics = new EvalRegressionBatchOp()
      .setLabelCol("Profit")
      .setPredictionCol("pred")
      .linkFrom(predictBatch)
      .collectMetrics()

    val rmse = metrics.getRmse.formatted("%.2f")
    model.save(f"model/DecisionTreeRegressor-$rmse(RMSE).csv")
    BatchOperator.execute()

    println("Total Samples Number:", metrics.getCount())
    println("SSE:", metrics.getSse())
    println("SAE:", metrics.getSae())
    println("RMSE:", metrics.getRmse())
    println("R2:", metrics.getR2())

  }
}

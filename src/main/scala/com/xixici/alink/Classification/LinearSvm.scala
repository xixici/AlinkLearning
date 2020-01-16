package com.xixici.alink.Classification

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp
import com.alibaba.alink.operator.batch.source._
import com.alibaba.alink.pipeline.Pipeline
import com.alibaba.alink.pipeline.classification._
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler
import com.alibaba.alink.pipeline.feature.OneHotEncoder

/**
  * Created by yang.lei01 on 2020/1/16.
  */
object LinearSvm {
  def main(args: Array[String]): Unit = {
    val schema
      : String = "age bigint, workclass string, fnlwgt bigint, education string, " + "education_num bigint, marital_status string, occupation string, " + "relationship string, race string, sex string, capital_gain bigint, " + "capital_loss bigint, hours_per_week bigint, native_country string, label string"
    val trainData = new CsvSourceBatchOp()
      .setFilePath("data/adult_train.csv")
      .setSchemaStr(schema)
    val testData = new CsvSourceBatchOp()
      .setFilePath("data/adult_test.csv")
      .setSchemaStr(schema)

    val svm = new LinearSvm()
      .setVectorCol("vec")
      .setLabelCol("label")
      .setPredictionCol("pred")
      .setPredictionDetailCol("detail")
    val onehot = new OneHotEncoder()
      .setSelectedCols("workclass",
                       "education",
                       "marital_status",
                       "occupation",
                       "relationship",
                       "race",
                       "sex",
                       "native_country")
      .setOutputCol("output")
      .setReservedCols("age",
                       "fnlwgt",
                       "education_num",
                       "capital_gain",
                       "capital_loss",
                       "hours_per_week",
                       "label")
    val assembler = new VectorAssembler()
      .setSelectedCols("output",
                       "age",
                       "fnlwgt",
                       "education_num",
                       "capital_gain",
                       "capital_loss",
                       "hours_per_week")
      .setOutputCol("vec")
      .setReservedCols("label")
    val pipeline = new Pipeline().add(onehot).add(assembler)

    val model = pipeline.add(svm).fit(trainData)
    val predictBatch = model.transform(testData)
    val metrics = new EvalBinaryClassBatchOp()
      .setLabelCol("label")
      .setPredictionDetailCol("detail")
      .linkFrom(predictBatch)
      .collectMetrics()

    val acc = (metrics.getAccuracy * 100).formatted("%.2f")
    model.save(f"model/LinearSvm-$acc%%(Acc).csv")
    BatchOperator.execute()

    println("AUC:", metrics.getAuc())
    println("KS:", metrics.getKs())
    println("PRC:", metrics.getPrc())
    println("Accuracy:", metrics.getAccuracy())
    println("Macro Precision:", metrics.getMacroPrecision())
    println("Micro Recall:", metrics.getMicroRecall())
    println("Weighted Sensitivity:", metrics.getWeightedSensitivity())
  }
}

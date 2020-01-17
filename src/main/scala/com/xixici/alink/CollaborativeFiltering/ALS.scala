package com.xixici.alink.CollaborativeFiltering

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp
import com.alibaba.alink.pipeline.Pipeline
import com.alibaba.alink.pipeline.recommendation.ALS

/**
  * Created by yang.lei01 on 2020/1/17.
  */
object ALS {
  def main(args: Array[String]): Unit = {
    val url = "data/movielens_ratings.csv"
    val schema =
      "userid bigint, movieid bigint, rating double, timestamp string"
    val data = new CsvSourceBatchOp().setFilePath(url).setSchemaStr(schema)

    val spliter = new SplitBatchOp().setFraction(0.8)
    spliter.linkFrom(data)

    val trainData = spliter
    val testData = spliter.getSideOutput(0)

    val als = new ALS()
      .setUserCol("userid")
      .setItemCol("movieid")
      .setRateCol("rating")
      .setNumIter(10)
      .setRank(10)
      .setLambda(0.1)
      .setPredictionCol("pred")
    val pipeline = new Pipeline()

    val model = pipeline.add(als).fit(trainData)
    val predictBatch = model.transform(testData)
    predictBatch.firstN(10).print()
    model.save(f"model/ALS.csv")
    BatchOperator.execute()
  }
}

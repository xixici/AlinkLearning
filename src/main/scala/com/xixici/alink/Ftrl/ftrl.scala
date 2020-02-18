package com.xixici.alink.Ftrl

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp
import com.alibaba.alink.operator.stream.dataproc.SplitStreamOp
import com.alibaba.alink.operator.stream.onlinelearning.{FtrlPredictStreamOp, FtrlTrainStreamOp}
import com.alibaba.alink.operator.stream.source.Kafka011SourceStreamOp
import com.alibaba.alink.pipeline.dataproc.StandardScaler
import com.alibaba.alink.pipeline.feature.FeatureHasher
import com.alibaba.alink.pipeline.{Pipeline, PipelineModel}

/**
  * Created by yang.lei01 on 2020/2/10.
  */
object ftrl {
  def main(args: Array[String]): Unit = {
    // schema of train data
    val schemaStr =
      "id string, click string, dt string, C1 string, banner_pos int, site_id string, site_domain string, site_category string, app_id string, app_domain string, app_category string, device_id string, device_ip string, device_model string, device_type string, device_conn_type string, C14 int, C15 int, C16 int, C17 int, C18 int, C19 int, C20 int, C21 int"
    val schemaArray =
      Array(
        "id",
        "click",
        "dt",
        "C1",
        "banner_pos",
        "site_id",
        "site_domain",
        "site_category",
        "app_id",
        "app_domain",
        "app_category",
        "device_id",
        "device_ip",
        "device_model",
        "device_type",
        "device_conn_type",
        "C14",
        "C15",
        "C16",
        "C17",
        "C18",
        "C19",
        "C20",
        "C21"
      )

    // prepare batch train data
    val batchTrainDataFn =
      "data/avazu-small.csv"
    val trainBatchData: CsvSourceBatchOp = new CsvSourceBatchOp()
      .setFilePath(batchTrainDataFn)
      .setSchemaStr(schemaStr)
      .setIgnoreFirstLine(true)
    // feature fit
    val labelColName = "click"
    val vecColName = "vec"
    val numHashFeatures = 30000

    val data0 = new Kafka011SourceStreamOp()
      .setBootstrapServers("127.0.0.1:9092")
      .setTopic("avazu")
      .setStartupMode("EARLIEST")
      .setGroupId("alink")
    //    data0.print(1,1000)
    //    StreamOperator.execute()
    // split stream to train and eval data
    val spliter = new SplitStreamOp().setFraction(0.5).linkFrom(data0)
    val train_stream_data: SplitStreamOp = spliter
    val test_stream_data = spliter.getSideOutput(0)
    val standardScaler = new StandardScaler()
      .setSelectedCols("C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21")
    val featureHasher = new FeatureHasher()
      .setSelectedCols(
        "C1",
        "banner_pos",
        "site_category",
        "app_domain",
        "app_category",
        "device_type",
        "device_conn_type",
        "C14",
        "C15",
        "C16",
        "C17",
        "C18",
        "C19",
        "C20",
        "C21",
        "site_id",
        "site_domain",
        "device_id",
        "device_model"
      )
      .setCategoricalCols("C1",
        "banner_pos",
        "site_category",
        "app_domain",
        "app_category",
        "device_type",
        "device_conn_type",
        "site_id",
        "site_domain",
        "device_id",
        "device_model")
      .setOutputCol(vecColName)
      .setNumFeatures(numHashFeatures)
    val feature_pipeline = new Pipeline()
      .add(standardScaler)
      .add(featureHasher)

    // fit and save feature pipeline model
    val FEATURE_PIPELINE_MODEL_FILE = "model/feature_pipe_model.csv"
    val feature_pipelineModel: PipelineModel =
      feature_pipeline.fit(trainBatchData)
    feature_pipelineModel.save(FEATURE_PIPELINE_MODEL_FILE)

    BatchOperator.execute()
    //    StreamOperator.execute()
    // load pipeline model
    //        val feature_pipelineModel = PipelineModel.load(FEATURE_PIPELINE_MODEL_FILE)
    //    val feature_pipelineModel = feature_pipeline.fit(trainBatchData)
    // train initial batch model\
    // TODO: something wrong 
    val lr = new LogisticRegressionTrainBatchOp()
    val fData =
      feature_pipelineModel.transform(trainBatchData)
    val initModel = lr
      .setVectorCol(vecColName)
      .setLabelCol(labelColName)
      .setWithIntercept(true)
      .setMaxIter(10)
      .linkFrom(fData)
    val sTrainData = feature_pipelineModel.transform(train_stream_data)
    val sTestData = feature_pipelineModel.transform(test_stream_data)
    // ftrl train
    val model = new FtrlTrainStreamOp(initModel)
      .setVectorCol(vecColName)
      .setLabelCol(labelColName)
      .setWithIntercept(true)
      .setAlpha(0.1)
      .setBeta(0.1)
      .setL1(0.01)
      .setL2(0.01)
      .setTimeInterval(10)
      .setVectorSize(numHashFeatures)
      .linkFrom(sTrainData)

    val predResult = new FtrlPredictStreamOp(initModel)
      .setVectorCol(vecColName)
      .setPredictionCol("pred")
      .setReservedCols("click")
      .setPredictionDetailCol("details")
      .linkFrom(model, sTestData)
    println(predResult)
    //predResult.print(key="predResult", refreshInterval = 30, maxLimit=20)

  }

}

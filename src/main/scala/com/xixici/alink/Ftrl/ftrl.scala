package com.xixici.alink.Ftrl

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp
import com.alibaba.alink.operator.stream.StreamOperator
import com.alibaba.alink.operator.stream.dataproc.{JsonValueStreamOp, SplitStreamOp}
import com.alibaba.alink.operator.stream.onlinelearning.{FtrlPredictStreamOp, FtrlTrainStreamOp}
import com.alibaba.alink.operator.stream.source.KafkaSourceStreamOp
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
    val schemaJsonArray: Array[String] =
      Array(
        "$.id",
        "$.click",
        "$.dt",
        "$.C1",
        "$.banner_pos",
        "$.site_id",
        "$.site_domain",
        "$.site_category",
        "$.app_id",
        "$.app_domain",
        "$.app_category",
        "$.device_id",
        "$.device_ip",
        "$.device_model",
        "$.device_type",
        "$.device_conn_type",
        "$.C14",
        "$.C15",
        "$.C16",
        "$.C17",
        "$.C18",
        "$.C19",
        "$.C20",
        "$.C21"
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

    // prepare stream train data with kafka
    val source = new KafkaSourceStreamOp()
      .setBootstrapServers("127.0.0.1:9092")
      .setTopic("avazu")
      .setStartupMode("GROUP_OFFSETS")
      .setGroupId("alink_group")
    val data = source
      .link(
        new JsonValueStreamOp()
          .setSelectedCol("message")
          .setReservedCols("")
          .setOutputCols(
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
          .setJsonPath(schemaJsonArray))
      .select(
        "id, "
          + "click, "
          + "dt, "
          + "C1, "
          + "CAST(banner_pos AS int) AS banner_po, "
          + "site_id, "
          + "site_domain, "
          + "site_category, "
          + "app_id, "
          + "app_domain, "
          + "app_category, "
          + "device_id, "
          + "device_ip, "
          + "device_model, "
          + "device_type, "
          + "device_conn_type, "
          + "CAST(C14 AS int) AS C14, "
          + "CAST(C15 AS int) AS C15, "
          + "CAST(C16 AS int) AS C16, "
          + "CAST(C17 AS int) AS C17, "
          + "CAST(C18 AS int) AS C18, "
          + "CAST(C19 AS int) AS C19, "
          + "CAST(C20 AS int) AS C20, "
          + "CAST(C21 AS int) AS C21"
      ).as(schemaArray)
    data.print(10, 20)

    // Type2: prepare stream train data with csv
    //    val wholeDataFile = "data/avazu-ctr-train-8M.csv"
    //    val data = new CsvSourceStreamOp()
    //      .setFilePath(wholeDataFile)
    //      .setSchemaStr(schemaStr)
    //      .setIgnoreFirstLine(true)

    // split stream to train and eval data
    val spliter = new SplitStreamOp().setFraction(0.5).linkFrom(data)
    val trainStreamData: SplitStreamOp = spliter
    val testStreamData: StreamOperator[_] = spliter.getSideOutput(0)

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
    val FEATURE_PIPELINE_MODEL_FILE: String = "model/feature_pipe_model.csv"
    val featurePipeline: PipelineModel =
      feature_pipeline.fit(trainBatchData)
    featurePipeline.save(FEATURE_PIPELINE_MODEL_FILE)

    BatchOperator.execute()

    // load pipeline model
    val featurePipelineModel = PipelineModel.load(FEATURE_PIPELINE_MODEL_FILE)

    // train initial batch model
    val lr = new LogisticRegressionTrainBatchOp()

    val fData: BatchOperator[_ <: BatchOperator[_]] =
      featurePipelineModel.transform(trainBatchData)
    // Use as to transform StreamOperator[_] to StreamOperator[_ <: StreamOperator[_]]
    val sTrainData: StreamOperator[_ <: StreamOperator[_]] =
      featurePipelineModel.transform(trainStreamData).as(schemaArray)
    val sTestData =
      featurePipelineModel.transform(testStreamData).as(schemaArray)

    val initModel = lr
      .setVectorCol(vecColName)
      .setLabelCol(labelColName)
      .setWithIntercept(true)
      .setMaxIter(10)
      .linkFrom(fData)

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

    predResult.print(30, 20)

    StreamOperator.execute()

  }

}

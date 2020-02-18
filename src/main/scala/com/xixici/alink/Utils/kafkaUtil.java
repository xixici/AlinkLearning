package com.xixici.alink.Utils;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.Kafka011SinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.source.Kafka011SourceStreamOp;

/**
 * Created by yang.lei01 on 2020/2/17.
 */
public class kafkaUtil {
    private static void write() throws Exception {
//        String URL = "data/iris.csv";
//        String SCHEMA_STR
//                = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
        String URL = "data/avazu-ctr-train-8M.csv";
        String SCHEMA_STR =
                "id string, click string, dt string, C1 string, banner_pos int, site_id string, site_domain string, site_category string, app_id string, app_domain string, app_category string, device_id string, device_ip string, device_model string, device_type string, device_conn_type string, C14 int, C15 int, C16 int, C17 int, C18 int, C19 int, C20 int, C21 int";
        CsvSourceStreamOp data = new CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR).setIgnoreFirstLine(true);

        Kafka011SinkStreamOp sink = new Kafka011SinkStreamOp()
                .setBootstrapServers("127.0.0.1:9092")
                .setDataFormat("json")
                .setTopic("avazu");

        data.link(sink);

        StreamOperator.execute();
    }

    private static void read() throws Exception {
        Kafka011SourceStreamOp source = new Kafka011SourceStreamOp()
                .setBootstrapServers("127.0.0.1:9092")
                .setTopic("iris")
                .setStartupMode("EARLIEST")
                .setGroupId("alink_group");

        source.print();

        StreamOperator.execute();
    }

    public static void main(String[] args) throws Exception {
        write();
//        read();

    }
}

package com.ms.kafkasink2hbase;

import com.ms.kafkasink2hbase.bean.Calllog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class Bootstrap {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","192.168.126.128:9092");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,"wxwmd");

        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList("calllog"));

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "192.168.126.128:2181");

        try {
            Connection connection= ConnectionFactory.createConnection(hbaseConf);
            Table callLogTable = connection.getTable(TableName.valueOf("call_log"));

            while(true){
                ConsumerRecords<String,String> callLogs=kafkaConsumer.poll(100);
                List<Put> callLogSink = new ArrayList<>();
                for (ConsumerRecord record:callLogs){
                    Calllog calllog=JSONObject.parseObject((String) record.value(), Calllog.class);
                    String rowkey=calllog.getCall1()+"-"+calllog.getCall2()+"-"+calllog.getCalltime();
                    Put put=new Put(rowkey.getBytes());
                    put.addColumn("caller".getBytes(),"caller1".getBytes(),calllog.getCall1().getBytes());
                    put.addColumn("caller".getBytes(),"caller2".getBytes(),calllog.getCall2().getBytes());
                    put.addColumn("info".getBytes(),"calltime".getBytes(),calllog.getCalltime().getBytes());
                    put.addColumn("info".getBytes(),"duration".getBytes(),calllog.getDuration().getBytes());
                    callLogSink.add(put);
                }
                callLogTable.put(callLogSink);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

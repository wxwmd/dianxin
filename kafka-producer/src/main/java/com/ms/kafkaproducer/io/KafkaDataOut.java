package com.ms.kafkaproducer.io;

import com.ms.common.bean.DataOut;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.Properties;


/**
 * 数据直接写入kafka
 * */
public class KafkaDataOut implements DataOut {
    private final KafkaProducer<String,String> kafkaProducer;
    private String topic;

    public KafkaDataOut(String topic) {
        this.topic=topic;
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","192.168.126.128:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer= new KafkaProducer<>(kafkaProps);
    }

    @Override
    public void setPath(String path) {

    }

    @Override
    public void write(Object data) throws Exception {
        ProducerRecord<String,String> callRecord=new ProducerRecord<>(topic, JSON.toJSONString(data));
        kafkaProducer.send(callRecord);
    }

    @Override
    public void write(String data) throws Exception {

    }

    @Override
    public void close() throws IOException {

    }
}

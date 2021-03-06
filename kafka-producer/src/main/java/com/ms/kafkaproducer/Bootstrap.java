package com.ms.kafkaproducer;


import com.ms.common.bean.Producer;
import com.ms.kafkaproducer.bean.KafkaCallRecordProducer;
import com.ms.kafkaproducer.io.KafkaDataOut;
import com.ms.kafkaproducer.io.LocalFileDataIn;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws  Exception {

        if ( args.length < 2 ) {
            System.out.println("系统参数不正确，请按照指定格式传递：java -jar Produce.jar path1 topic");
            System.exit(1);
        }

        // 构建生产者对象
        Producer producer = new KafkaCallRecordProducer();

//        producer.setIn(new LocalFileDataIn("E:\\bigdata\\course\\13_尚硅谷大数据技术之电信客服\\2.资料\\辅助文档\\contact.log"));
//        producer.setOut(new LocalFileDataOut("E:\\bigdata\\course\\13_尚硅谷大数据技术之电信客服\\2.资料\\辅助文档\\call.log"));

        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new KafkaDataOut(args[1]));

        // 生产数据
        producer.produce();

        // 关闭生产者对象
        producer.close();
    }
}

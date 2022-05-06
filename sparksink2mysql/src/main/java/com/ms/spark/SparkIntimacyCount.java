package com.ms.spark;

import com.ms.spark.bean.CallLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class SparkIntimacyCount {
    public static void main(String[] args) {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(TableInputFormat.INPUT_TABLE,"call_log");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("calculate intimacy");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<ImmutableBytesWritable, Result> callLogRDD = sparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);

        JavaRDD<CallLog> callLogJavaRDD = callLogRDD.map((immutableBytesWritableResultTuple2)->{
            Result result = immutableBytesWritableResultTuple2._2();
            String caller1 = Bytes.toString(result.getValue("caller".getBytes(), "caller1".getBytes()));
            String caller2 = Bytes.toString(result.getValue("caller".getBytes(), "caller2".getBytes()));
            String callTime = Bytes.toString(result.getValue("info".getBytes(), "calltime".getBytes()));
            int duration = Bytes.toInt(result.getValue("info".getBytes(), "duration".getBytes()));
            return new CallLog(caller1, caller2, callTime, duration);
        });

        JavaRDD<Tuple3<String,String,Integer>> intimacyCount = callLogJavaRDD.map((callLog -> {
            String caller1 = callLog.getCaller1();
            String caller2 = callLog.getCaller2();
            return Tuple3.apply(caller1, caller2, 1);
        }))
                .keyBy(tuple3 -> Tuple2.apply(tuple3._1(), tuple3._2()))
                .reduceByKey((tuple3, tuple32) -> Tuple3.apply(tuple3._1(), tuple3._2(), tuple3._3() + tuple32._3()))
                .map((Tuple2::_2));

        intimacyCount.foreach(x->{
            System.out.println(x.toString());
        });
    }
}

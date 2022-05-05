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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Iterator;
import java.util.Locale;

public class SparkHbase {
    public static void main(String[] args) {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(TableInputFormat.INPUT_TABLE,"call_log");

        SparkConf sparkConf=new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("spark connect hbase");
        JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);

        JavaPairRDD<ImmutableBytesWritable, Result> callLogRDD = sparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);

        callLogRDD.cache();

        JavaRDD<CallLog> callLogJavaRDD = callLogRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, CallLog>() {
            @Override
            public CallLog call(Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                Result result = resultTuple2._2();
                String caller1 = Bytes.toString(result.getValue("caller".getBytes(), "caller1".getBytes()));
                String caller2 = Bytes.toString(result.getValue("caller".getBytes(), "caller2".getBytes()));
                String callTime = Bytes.toString(result.getValue("info".getBytes(), "calltime".getBytes()));
                int duration = Bytes.toInt(result.getValue("info".getBytes(), "duration".getBytes()));
                return new CallLog(caller1, caller2, callTime, duration);
            }
        });

        // 想要统计每个号码每个月的通话次数
        JavaRDD<Tuple3<String, String, Integer>> resultRDD = callLogJavaRDD.map(new Function<CallLog, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> call(CallLog callLog) throws Exception {
                String caller = callLog.getCaller1();
                String month = callLog.getCallTime().substring(0, 6);
                return Tuple3.apply(caller, month, 1);
            }
        }).keyBy(new Function<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Tuple3<String, String, Integer> tuple3) throws Exception {
                return Tuple2.apply(tuple3._1(), tuple3._2());
            }
        }).reduceByKey(new Function2<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> call(Tuple3<String, String, Integer> tuple31, Tuple3<String, String, Integer> tuple32) throws Exception {
                return Tuple3.apply(tuple31._1(), tuple31._2(), tuple31._3() + tuple32._3());
            }
        }).map(new Function<Tuple2<Tuple2<String, String>, Tuple3<String, String, Integer>>, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> call(Tuple2<Tuple2<String, String>, Tuple3<String, String, Integer>> tuple2) throws Exception {
                return tuple2._2();
            }
        });
        resultRDD.collect().forEach(System.out::println);

        // 将结果插入到mysql中
        String mysqlDriver = "com.mysql.jdbc.Driver";
        String mysqlUrl = "jdbc:mysql://localhost:3306/dianxin";
        String userName = "root";
        String passWd = "w2000x0322w";

        resultRDD.foreachPartition(new VoidFunction<Iterator<Tuple3<String, String, Integer>>>() {
            @Override
            public void call(Iterator<Tuple3<String, String, Integer>> results) throws Exception {
                Connection mysqlConnection = DriverManager.getConnection(mysqlUrl, userName, passWd);
                String sql="insert into tb_month_tel(caller,month,sum_count) values(%s,%s,%d)";
                while (results.hasNext()){
                    Tuple3<String, String, Integer> result = results.next();
                    String insertSQL = String.format(sql, result._1(), result._2(), result._3());
                    mysqlConnection.prepareStatement(insertSQL).executeUpdate();
                }
            }
        });
    }
}

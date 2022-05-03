package com.ms.kafkasink2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

class HbaseTest{
    static Configuration conf = null;
    static Connection conn = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "ubuntu");
        conf.set("hbase.zookeeper.property.client", "2181");

        conf.set("hbase.client.pause", "50");
        conf.set("hbase.client.retries.number", "3");
        conf.set("hbase.rpc.timeout", "2000");
        conf.set("hbase.client.operation.timeout", "3000");
        conf.set("hbase.client.scanner.timeout.period", "10000");
        try{
            conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 创建只有一个列簇的表
     * @throws Exception
     */
    public static void createTable() throws Exception{
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(TableName.valueOf("test"))){
            TableName tableName = TableName.valueOf("test");
            //表描述器构造器

            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述器构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user"));
            //获得列描述器
            ColumnFamilyDescriptor cfd = cdb.build();
            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);

        }else {
            System.out.println("表已存在");
        }
        //关闭连接
    }

    public static void main(String[] args) throws IOException {
        Admin admin = conn.getAdmin();
        System.out.println(admin.tableExists(TableName.valueOf("test")));
    }
}
package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoadHFiles {
    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        /*连接HBase*/
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.set("hbase.zookeeper.quorum", "172.31.42.237,172.31.43.12,172.31.43.21");
        //configuration.set("hbase.zookeeper.quorum", "192.168.61.130,192.168.61.131,192.168.61.132");
        configuration.set("hbase.zookeeper.quorum", "47.98.176.164,47.98.47.81,116.62.119.79");
        //configuration.set("hbase.master", "172.31.42.237:60010");
        //configuration.set("hbase.master", "192.168.61.130:60010");
        configuration.set("hbase.master", "47.98.176.164:60010");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin=connection.getAdmin();
        TableName tableName=TableName.valueOf("userBehaviors");
        /* 删除旧表
         * 如果存在要创建的表，那么先删除，再创建*/
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        /*创建新表*/
        /*方式一*/
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<ColumnFamilyDescriptor>();
        List<String> columnFamilies = new ArrayList<String>(Arrays.asList("info"));
        for (String columnFamily : columnFamilies) {
            //列族描述起构造器
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            //获得列描述
            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
            columnFamilyDescriptorList.add(columnFamilyDescriptor);
        }
        // 设置列簇
        tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptorList);
        //获得表描述器
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tableDescriptor);

        /*加载HFile到HBase*/
        //configuration.set("fs.defaultFS","hdfs://47.98.176.164:9000");
        try{
            LoadIncrementalHFiles Loader = new LoadIncrementalHFiles(configuration);
            Loader.doBulkLoad(new Path("hdfs://47.98.176.164:9000/test/experiment/userBehaviorsHFile"), admin, connection.getTable(tableName), connection.getRegionLocator(tableName));
            System.out.println("Operation finished!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

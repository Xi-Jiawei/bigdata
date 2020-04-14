package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BulkLoadDemo {
    static class MapreduceMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable Key, Text line, Context context) throws IOException, InterruptedException {
            String[] strArray = line.toString().split("\u0001");

            //int rowkeyStr=Integer.parseInt(strArray[0]);
            String rowkeyStr=strArray[0];
            for(int i=0;i<9-strArray[0].length();i++)rowkeyStr="0"+rowkeyStr;
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(rowkeyStr));//封装rowkey
            Put put = new Put(Bytes.toBytes(rowkeyStr));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes(strArray[1]));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(strArray[2]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("articleid"), Bytes.toBytes(strArray[3]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), Bytes.toBytes(strArray[4]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(strArray[5]));//level
            if(!strArray[6].equals(""))put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("degree"), Bytes.toBytes(strArray[6]));//degree
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("domain"), Bytes.toBytes(strArray[7]));//domain
            context.write(rowkey, put);
            /*//int rowkeyStr=Integer.parseInt(strArray[0]);
            String rowkeyStr=strArray[0];
            for(int i=0;i<9-strArray[0].length();i++)rowkeyStr="0"+rowkeyStr;
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(rowkeyStr));//封装rowkey
            Put put = new Put(Bytes.toBytes(rowkeyStr));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes(strArray[1]));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(Integer.parseInt(strArray[2])));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("articleid"), Bytes.toBytes(strArray[3]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), Bytes.toBytes(strArray[4]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(Integer.parseInt(strArray[5])));//level
            if(!strArray[6].equals(""))put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("degree"), Bytes.toBytes(Integer.parseInt(strArray[6])));//degree
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("domain"), Bytes.toBytes(strArray[7]));//domain
            context.write(rowkey, put);*/
        }
    }

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

        /*使用MapReduce生成HFile文件*/
        //配置yarn任务
        //configuration.set("fs.defaultFS","hdfs://192.168.61.130:9000");
        configuration.set("fs.defaultFS","hdfs://47.98.176.164:9000");
        //FileSystem fileSystem= FileSystem.get(configuration);
        //fileSystem.delete(new Path("/test/experiment/userBehaviorsHFile"),true);
        Job job = Job.getInstance(configuration,"BulkLoadDemo");
        job.setJarByClass(BulkLoadDemo.class);
        job.setMapperClass(MapreduceMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        FileInputFormat.setInputPaths(job, "/test/experiment/userBehaviors");
        FileOutputFormat.setOutputPath(job, new Path("/test/experiment/userBehaviorsHFile"));
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName),connection.getRegionLocator(tableName));
        try {
            //提交执行yarn任务
            System.out.println("Job start");
            if(job.waitForCompletion(true)) {
                System.out.println("Job end");
                //加载HFile到HBase
                LoadIncrementalHFiles Loader = new LoadIncrementalHFiles(configuration);
                Loader.doBulkLoad(new Path("/test/experiment/userBehaviorsHFile"), admin, connection.getTable(tableName), connection.getRegionLocator(tableName));
                System.out.println("Operation finished!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

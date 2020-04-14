package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HBaseDemo {
    public static void main(String[] args) {
        Configuration configuration;
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.set("hbase.zookeeper.quorum", "172.31.42.237,172.31.43.12,172.31.43.21");
        configuration.set("hbase.zookeeper.quorum", "192.168.61.130,192.168.61.131,192.168.61.132");
        //configuration.set("hbase.zookeeper.quorum", "47.98.176.164,47.98.47.81,116.62.119.79");
        //configuration.set("hbase.master", "172.31.42.237:60010");
        configuration.set("hbase.master", "192.168.61.130:60010");
        //configuration.set("hbase.master", "47.98.176.164:60010");

        try {
            /*连接HBase*/
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            //String tableName="users";
            TableName tableName=TableName.valueOf("users");

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
            List<String> columnFamilies = new ArrayList<String>(Arrays.asList("basicInfo", "educationInfo"));
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
            /*方式二：过时的API，HBase 3.0及以上版本弃用此接口*/
            /*HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("basicInfo"));
            tableDescriptor.addFamily(new HColumnDescriptor("educationInfo"));
            admin.createTable(tableDescriptor);*/

            /*插入数据*/
            //单条插入
            Table table = connection.getTable(tableName);
            /*Put put = new Put(Bytes.toBytes("18140070"));
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("name"), Bytes.toBytes("LiuYan"));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("birthday"), Bytes.toBytes("1102"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("school"), Bytes.toBytes("北京交通大学"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("major"), Bytes.toBytes("软件工程"));
            table.put(put);*/
            //批量插入
            /*List<Put> putList=new ArrayList<Put>();
            Put put = new Put(Bytes.toBytes("18140070"));
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("name"), Bytes.toBytes("LiuYan"));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("birthday"), Bytes.toBytes("1102"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("school"), Bytes.toBytes("北京交通大学"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("major"), Bytes.toBytes("软件工程"));
            putList.add(put);
            put = new Put(Bytes.toBytes("18140039"));
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("name"), Bytes.toBytes("XiJiawei"));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("birthday"), Bytes.toBytes("0806"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("school"), Bytes.toBytes("北京交通大学"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("major"), Bytes.toBytes("计算机技术"));
            putList.add(put);
            table.put(putList);*/
            //手动执行flush：BufferedMutator替代Table进行插入
            BufferedMutatorParams bufferedMutatorParams=new BufferedMutatorParams(tableName).writeBufferSize(1024 * 1024);//设置1M缓冲区，缓冲区满后数据会自动flush
            BufferedMutator bufferedMutator=connection.getBufferedMutator(bufferedMutatorParams);
            Put put = new Put(Bytes.toBytes("18140070"));
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("name"), Bytes.toBytes("LiuYan"));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("birthday"), Bytes.toBytes("1102"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("school"), Bytes.toBytes("北京交通大学"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("major"), Bytes.toBytes("软件工程"));
            bufferedMutator.mutate(put);
            put = new Put(Bytes.toBytes("18140039"));
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("name"), Bytes.toBytes("XiJiawei"));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("basicInfo"), Bytes.toBytes("birthday"), Bytes.toBytes("0806"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("school"), Bytes.toBytes("北京交通大学"));
            put.addColumn(Bytes.toBytes("educationInfo"), Bytes.toBytes("major"), Bytes.toBytes("计算机技术"));
            bufferedMutator.mutate(put);
            bufferedMutator.flush();

            /*浏览整表*/
            Scan scan = new Scan();
            ResultScanner resutScanner = table.getScanner(scan);
            for(Result result: resutScanner){
                System.out.println("scan:  " + result);
            }

            /*按行查询*/
            Get get = new Get(Bytes.toBytes("18140070"));
            //先判断是否有此条数据
            if(!get.isCheckExistenceOnly()){
                System.out.println("rowkey: 18140070");

                // 按rowkey查询整行内容
                /*Result result = table.get(get);
                for (Cell cell : result.rawCells()) {
                    String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.print("columnName: "+columnName+", ");
                    System.out.println("value: "+value);
                }*/

                // 按rowkey查询行内指定列
                Result result = table.get(get);
                byte[] resultByte = result.getValue(Bytes.toBytes("basicInfo"),Bytes.toBytes("name"));
                System.out.println("result: "+Bytes.toString(resultByte));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

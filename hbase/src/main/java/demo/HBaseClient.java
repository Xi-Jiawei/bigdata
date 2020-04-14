package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HBaseClient {
    public static void main(String[] args) throws Throwable{
        Configuration configuration;
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.set("hbase.zookeeper.quorum", "172.31.42.237,172.31.43.12,172.31.43.21");
        //configuration.set("hbase.zookeeper.quorum", "192.168.61.130,192.168.61.131,192.168.61.132");
        configuration.set("hbase.zookeeper.quorum", "47.98.176.164,47.98.47.81,116.62.119.79");
        //configuration.set("hbase.master", "172.31.42.237:60010");
        //configuration.set("hbase.master", "192.168.61.130:60010");
        configuration.set("hbase.master", "47.98.176.164:60010");

        try {
            /*连接HBase*/
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            //String tableName="users";
            TableName tableName=TableName.valueOf("userBehaviors");

            /* 删除旧表
             * 如果存在要创建的表，那么先删除，再创建*/
            /*if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }*/

            /*创建新表*/
            /*方式一*/
            /*TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
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
            admin.createTable(tableDescriptor);*/
            /*方式二：过时的API，HBase 3.0及以上版本弃用此接口*/
            /*HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("basicInfo"));
            tableDescriptor.addFamily(new HColumnDescriptor("educationInfo"));
            admin.createTable(tableDescriptor);*/

            /*插入数据*/
            Table table = connection.getTable(tableName);
            /*Put put = new Put(Bytes.toBytes(1));
            //put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes("U0000001"));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes(1));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(0));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("articleid"), Bytes.toBytes("D0607203"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), Bytes.toBytes("2020-02-03 16:49:00.0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(2));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("degree"), Bytes.toBytes(3));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("domain"), Bytes.toBytes("图像识别"));
            table.put(put);
            put = new Put(Bytes.toBytes(2));
            //put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes("U0000002"));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes(2));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(1));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("articleid"), Bytes.toBytes("D0823792"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), Bytes.toBytes("2020-02-03 16:57:00.0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(3));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("degree"), Bytes.toBytes(3));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("domain"), Bytes.toBytes("大数据存储"));
            table.put(put);
            put = new Put(Bytes.toBytes(3));
            //put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes("U0000003"));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes(3));//参数：1.列族名  2.列名  3.值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(2));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("articleid"), Bytes.toBytes("D0547663"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), Bytes.toBytes("2020-02-03 17:26:00.0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(1));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("domain"), Bytes.toBytes("大数据存储"));
            table.put(put);*/

            /*浏览整表*/
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            /*for(Result result: resultScanner){
                System.out.println("scan:  " + result);
            }*/

            /*按行查询*/
            //Get get = new Get(Bytes.toBytes(2));
            Get get = new Get(Bytes.toBytes("000000002"));
            //先判断是否有此条数据
            if(!get.isCheckExistenceOnly()){
                System.out.println("rowkey: 000000002");

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
                byte[] resultByte = result.getValue(Bytes.toBytes("info"),Bytes.toBytes("userid"));
                System.out.println("result: "+Bytes.toString(resultByte));
            }

            /*聚合*/
            AggregationClient aggregationClient = new AggregationClient(configuration);
            // 实例化一个Scan对象
            scan = new Scan();
            scan.addFamily(Bytes.toBytes("info"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"));
            // 获取行数
            long rowCount = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
            System.out.println("row count is " + rowCount);
            // 获取最大值
            scan = new Scan();
            scan.addFamily(Bytes.toBytes("info"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"));
            long max = aggregationClient.max(tableName, new LongColumnInterpreter(), scan);
            System.out.println("max number is " + max);
            // 获取最小值
            long min = aggregationClient.min(tableName, new LongColumnInterpreter(),scan);
            System.out.println("min number is " + min);

            /*过滤*/
            scan = new Scan();
            //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"));
            //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"));
            //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("degree"));
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            //SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behavior"), CompareOperator.EQUAL, Bytes.toBytes(1));
            //scan.setFilter(filter);
            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("2015-01-01 00:00:00.0")));
            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), CompareOperator.LESS_OR_EQUAL, Bytes.toBytes("2015-01-01 01:00:00.0")));
            //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behavior"), CompareOperator.EQUAL, Bytes.toBytes("1")));
            //注意，如果setFilterIfMissing标志设为真，如果该行没有指定的列，那么该行的所有列将不发出。缺省值为假。所以，如果不设置此项，某一行不含有该列，同样返回
            //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("0")));
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("0"));
            filter.setFilterIfMissing(true);
            filterList.addFilter(filter);
            scan.setFilter(filterList);
            resultScanner = table.getScanner(scan);
            int count=0;
            for (Result result=resultScanner.next();result != null;result=resultScanner.next()) {
                count++;
                for (Cell cell : result.rawCells()) {
                    System.out.print("rowkey: " + Bytes.toString(CellUtil.cloneRow(cell))+" ");
                    System.out.print("columnFamily: " + Bytes.toString(CellUtil.cloneFamily(cell))+" ");
                    System.out.print("column: " + Bytes.toString(CellUtil.cloneQualifier(cell))+" ");
                    System.out.println("value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            System.out.println("row count of behavior 1 between 2015-01-01 00:00:00.0 and 2015-01-01 01:00:00.0 is "+count);

            /*过滤+聚合*/
            /*发现个问题，Scan设置多过滤器时，如果指定了列（即使用scan.addColumn()添加指定列），那么协同器会只统计排最后的那列的过滤结果
            比如设置两过滤器，一个过滤"degree"列，一个过滤"behavior"列，aggregationClient.rowCount()接口函数会只统计"degree"列的过滤结果
            针对以上问题，尝试不使用scan.addColumn()添加指定列，发现结果统计结果正常了。
            也就是注释掉
            “scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"));”
            两列后，统计结果正常了*/
            scan = new Scan();
            //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"));
            //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"));
            filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("2015-01-01 00:00:00.0")));
            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), CompareOperator.LESS_OR_EQUAL, Bytes.toBytes("2015-01-01 01:00:00.0")));
            //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behavior"), CompareOperator.EQUAL, Bytes.toBytes("1")));
            //注意，如果setFilterIfMissing标志设为真，如果该行没有指定的列，那么该行的所有列将不发出。缺省值为假。所以，如果不设置此项，某一行不含有该列，同样返回
            //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("0")));
            filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("0"));
            filter.setFilterIfMissing(true);
            filterList.addFilter(filter);
            scan.setFilter(filterList);
            // 获取行数
            rowCount = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
            System.out.println("row count of behavior 1 between 2015-01-01 00:00:00.0 and 2015-01-01 01:00:00.0 is " + rowCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class HBaseAggregation {
    public static void main(String[] args) throws Throwable {
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

            /*聚合结果存入文件*/
            TableName tableName=TableName.valueOf("userBehaviors");// 获取TableName对象
            Scan scan = new Scan();// 实例化一个Scan对象
            SingleColumnValueFilter filter;// 单列过滤器
            FilterList filterList;// 过滤器
            // 获取行数
            AggregationClient aggregationClient = new AggregationClient(configuration);// 实例化一个AggregationClient对象
            long rowCount = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
            System.out.println("row count is " + rowCount);

            //BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("D:\\Users\\veev\\BigData\\userBehaviorsCount"))));
            BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/usr/local/src/userBehaviorsCount"))));

            // 时间递增测试
            /*SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar start= Calendar.getInstance();
            Calendar end= Calendar.getInstance();
            start.setTime(simpleDateFormat.parse("2015-01-01 00:00:00"));
            end.setTime(simpleDateFormat.parse("2015-01-01 23:59:59"));
            while (start.before(end)) {
                System.out.println(simpleDateFormat.format(start.getTime()));
                start.add(Calendar.HOUR, 1);
            }*/
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar begin = Calendar.getInstance();
            Calendar end = Calendar.getInstance();
            String startTime = "";
            String endTime = "";
            begin.setTime(simpleDateFormat.parse("2015-01-01 00:00:00"));
            end.setTime(simpleDateFormat.parse("2015-12-31 23:59:59"));
            while (begin.before(end)) {
                startTime = simpleDateFormat.format(begin.getTime());
                begin.add(Calendar.HOUR, 1);
                endTime = simpleDateFormat.format(begin.getTime());
                System.out.println("from " + startTime + " to " + endTime);
                SingleColumnValueFilter startTimeFilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes(startTime));
                SingleColumnValueFilter endTimeFilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), CompareOperator.LESS_OR_EQUAL, Bytes.toBytes(endTime));

                //level 0
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("level"), CompareOperator.EQUAL, Bytes.toBytes("0")));
                scan.setFilter(filterList);
                long level0 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of level 0 is " + level0);
                //level 1
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("level"), CompareOperator.EQUAL, Bytes.toBytes("1")));
                scan.setFilter(filterList);
                long level1 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of level 1 is " + level1);
                //level 2
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("level"), CompareOperator.EQUAL, Bytes.toBytes("2")));
                scan.setFilter(filterList);
                long level2 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of level 2 is " + level2);
                //level 3
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("level"), CompareOperator.EQUAL, Bytes.toBytes("3")));
                scan.setFilter(filterList);
                long level3 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of level 3 is " + level3);

                //degree -15
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("-15")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("-15"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degreeNeg15 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree -15 is " + degreeNeg15);
                //degree -10
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("-10")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("-10"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degreeNeg10 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree -10 is " + degreeNeg10);
                //degree -5
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("-5")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("-5"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degreeNeg5 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree -5 is " + degreeNeg5);
                //degree 0
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("0")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("0"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degree0 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree 0 is " + degree0);
                //degree 1
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("1")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("1"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degree1 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree 1 is " + degree1);
                //degree 2
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("2")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("2"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degree2 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree 2 is " + degree2);
                //degree 3
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("3")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("3"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degree3 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree 3 is " + degree3);
                //degree 4
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("4")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("4"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degree4 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree 4 is " + degree4);
                //degree 5
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("5")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("5"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degree5 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree 5 is " + degree5);
                //degree 6
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("6")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("6"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degree6 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree 6 is " + degree6);
                //degree 7
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                //filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("7")));
                filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("degree"), CompareOperator.EQUAL, Bytes.toBytes("7"));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
                scan.setFilter(filterList);
                long degree7 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of degree 7 is " + degree7);

                //behavior 0
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behavior"), CompareOperator.EQUAL, Bytes.toBytes("0")));
                scan.setFilter(filterList);
                long behavior0 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of behavior 0 is " + behavior0);
                //behavior 1
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behavior"), CompareOperator.EQUAL, Bytes.toBytes("1")));
                scan.setFilter(filterList);
                long behavior1 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of behavior 1 is " + behavior1);
                //behavior 2
                filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(startTimeFilter);
                filterList.addFilter(endTimeFilter);
                filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("behavior"), CompareOperator.EQUAL, Bytes.toBytes("2")));
                scan.setFilter(filterList);
                long behavior2 = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
                System.out.println("row count of behavior 2 is " + behavior2);

                /*存入Hive*/
                writer.write(startTime);
                writer.write("\u0001"+Long.toString(level0));
                writer.write("\u0001"+Long.toString(level1));
                writer.write("\u0001"+Long.toString(level2));
                writer.write("\u0001"+Long.toString(level3));
                writer.write("\u0001"+Long.toString(degreeNeg15));
                writer.write("\u0001"+Long.toString(degreeNeg10));
                writer.write("\u0001"+Long.toString(degreeNeg5));
                writer.write("\u0001"+Long.toString(degree0));
                writer.write("\u0001"+Long.toString(degree1));
                writer.write("\u0001"+Long.toString(degree2));
                writer.write("\u0001"+Long.toString(degree3));
                writer.write("\u0001"+Long.toString(degree4));
                writer.write("\u0001"+Long.toString(degree5));
                writer.write("\u0001"+Long.toString(degree6));
                writer.write("\u0001"+Long.toString(degree7));
                writer.write("\u0001"+Long.toString(behavior0));
                writer.write("\u0001"+Long.toString(behavior1));
                writer.write("\u0001"+Long.toString(behavior2)+"\n");

                writer.flush();
            }
            writer.close();
            System.out.println("Operation finished!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

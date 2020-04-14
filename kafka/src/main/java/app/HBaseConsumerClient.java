package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

/*
 * @for: experiment 3
 * @author: xijiawei
 * @from: 2019.12.23
 * */
public class HBaseConsumerClient {
    public static void main(String[] args) {
        try {
            /*连接HBase*/
            Configuration configuration;
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            //configuration.set("hbase.zookeeper.quorum", "172.31.42.237,172.31.43.12,172.31.43.21");
            //configuration.set("hbase.zookeeper.quorum", "192.168.61.130,192.168.61.131,192.168.61.132");
            configuration.set("hbase.zookeeper.quorum", "47.98.176.164,47.98.47.81,116.62.119.79");
            //configuration.set("hbase.master", "172.31.42.237:60010");
            //configuration.set("hbase.master", "192.168.61.130:60010");
            configuration.set("hbase.master", "47.98.176.164:60010");
            /*连接HBase*/
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
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

            /*连接MySQL*/
            java.sql.Connection con;
            //驱动程序名，加载驱动前要先添加mysql连接器
            String driver = "com.mysql.cj.jdbc.Driver";//String driver = "com.mysql.jdbc.Driver";
            //URL指向要访问的数据库名
            //String url = "jdbc:mysql://192.168.61.131:3306/mydb?serverTimezone=Asia/Shanghai";//连接指定数据库前先要创建此数据库："create database if not exists mydb;"。String url = "jdbc:mysql://172.31.43.13:3306/mydb"
            String url = "jdbc:mysql://47.98.47.81:3306/mydb?serverTimezone=Asia/Shanghai";//连接指定数据库前先要创建此数据库："create database if not exists mydb;"。String url = "jdbc:mysql://172.31.43.13:3306/mydb"
            //MySQL配置时的用户名
            String user = "root";
            //MySQL配置时的密码
            //String password = "314159";
            String password = "fionasit61";
            Class.forName(driver);//加载驱动程序
            con = DriverManager.getConnection(url, user, password);
            if (!con.isClosed())
                System.out.println("succeeded connecting to the database!");

            /*连接zookeeper集群，删除旧topic，创建新topic*/
            String topic="mykafka";
            Properties properties;
            AdminClient adminClient;
            properties = new Properties();
            //properties.put("bootstrap.servers", "172.31.42.237:9092,172.31.43.12:9092,172.31.43.21:9092");
            //properties.put("bootstrap.servers", "192.168.61.130:9092,192.168.61.131:9092,192.168.61.132:9092");
            properties.put("bootstrap.servers", "47.98.176.164:9092,47.98.47.81:9092,116.62.119.79:9092");
            adminClient = AdminClient.create(properties);
            //删除topic
            List<String> topics=new ArrayList<String>();
            topics.add(topic);
            adminClient.deleteTopics(topics);
            adminClient.close();
            //创建topic
            NewTopic newTopic = new NewTopic(topic,1,(short) 3);//partitions:1; replication-factor:3
            adminClient.createTopics(Arrays.asList(newTopic));
            adminClient.close();

            /*连接Kafka，启动消费者*/
            properties = new Properties();
            //properties.put("bootstrap.servers", "172.31.43.13:9092");//xxx是服务器集群的ip
            //properties.put("bootstrap.servers", "192.168.61.132:9092");//xxx是服务器集群的ip
            properties.put("bootstrap.servers", "47.98.176.164:9092");//xxx是服务器集群的ip
            properties.put("group.id", "test-consumer-group");
            properties.put("enable.auto.commit", "true");
            properties.put("auto.commit.interval.ms", "1000");
            properties.put("auto.offset.reset", "latest");
            properties.put("session.timeout.ms", "30000");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Arrays.asList(topic));

            /*插入hbase*/
            /*获取Table对象*/
            //Table table = connection.getTable(tableName);
            // BufferedMutator替代Table进行插入，关闭hbase自动flush，手动flush。这是关键设置，可以有效解决写入大量数据时内存爆满的问题
            // 旧版API：“HTable table=new HTable(conf,TableName.valueOf(tableName));table.setAutoFlush(false);table.setWriteBufferSize(1024 * 1024);table.put(put);table.flushCommits();”
            BufferedMutatorParams bufferedMutatorParams=new BufferedMutatorParams(tableName).writeBufferSize(32 * 1024 * 1024);//设置32M缓冲区，缓冲区满后数据会自动flush
            BufferedMutator bufferedMutator=connection.getBufferedMutator(bufferedMutatorParams);
            PreparedStatement preparedStatement;
            Put put;
            int index=0;
            ConsumerRecords<String, String> records;
            String messagetype="";
            String messagebody;
            String[] strArray;
            ResultSet resultSet;
            while (true) {
                // records = kafkaConsumer.poll(100);
                records = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));//接收消息
                for (ConsumerRecord<String, String> record : records) {
                    //通信协议格式形如：
                    // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                    String message=record.value();
                    //System.out.println("message "+index+": "+message);
                    if(message.contains("messagetype"))
                        messagetype=message.substring(message.indexOf("'messagetype'")+14,message.indexOf("}"));//indexOf 方法返回一个整数值，指出 String 对象内子字符串的开始位置。如果没有找到子字符串，则返回-1。
                    if(messagetype.equals("3")){
                        index++;
                        System.out.println(index);
                        messagebody=message.substring(message.indexOf("}")+1);
                        System.out.println(messagebody);
                        //String[] strArray = messagebody.split("\u0001");
                        strArray = messagebody.split("\\s+");//以"\s+"分割字符串

                        //插入数据
                        put = new Put(Bytes.toBytes(index));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes(strArray[0]));//参数：1.列族名  2.列名  3.值
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(Integer.parseInt(strArray[1])));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("articleid"), Bytes.toBytes(strArray[2]));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), Bytes.toBytes(strArray[3]+" "+strArray[4]));//date=strArray[3]+" "+strArray[4];

                        preparedStatement = con.prepareStatement("select level,degree from users where userid='"+strArray[0]+"';");
                        resultSet=preparedStatement.executeQuery();
                        if(resultSet.next()) {
                            int level = resultSet.getInt(1);//注意，MySQL API均以1开始，而不是以0开始
                            Object degree = resultSet.getObject(2);//int degree = resultSet.getInt(2);
                            System.out.println("Degree of user '"+strArray[0]+"': "+degree);

                            //插入数据
                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(level));
                            if(degree!=null)put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("degree"), Bytes.toBytes((int)degree));
                        }
                        resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                        preparedStatement = con.prepareStatement("select domain from articles where articleid='"+strArray[2]+"';");
                        resultSet=preparedStatement.executeQuery();
                        if(resultSet.next()) {
                            String domain = resultSet.getString(1);
                            //System.out.println("Domain of article '"+strArray[2]+"': "+domain);

                            //插入数据
                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("domain"), Bytes.toBytes(domain));
                        }
                        resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                        //table.put(put);
                        bufferedMutator.mutate(put);
                        if(index%10000==0)bufferedMutator.flush();
                        if(index%100000==0)System.gc();
                    }else if(messagetype.equals("-1")){
                        bufferedMutator.flush();
                        System.out.println("Operation finished!");
                    }
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}

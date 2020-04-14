package app;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FileConsumerClient {
    public static void main(String[] args) {
        try {
            /*连接MySQL*/
            java.sql.Connection con;
            //驱动程序名，加载驱动前要先添加mysql连接器
            String driver = "com.mysql.cj.jdbc.Driver";//String driver = "com.mysql.jdbc.Driver";
            //URL指向要访问的数据库名
            String url = "jdbc:mysql://192.168.61.131:3306/mydb?serverTimezone=Asia/Shanghai";//连接指定数据库前先要创建此数据库："create database if not exists mydb;"。String url = "jdbc:mysql://172.31.43.13:3306/mydb"
            //String url = "jdbc:mysql://47.98.47.81:3306/mydb?serverTimezone=Asia/Shanghai";//连接指定数据库前先要创建此数据库："create database if not exists mydb;"。String url = "jdbc:mysql://172.31.43.13:3306/mydb"
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
            properties.put("bootstrap.servers", "192.168.61.130:9092,192.168.61.131:9092,192.168.61.132:9092");
            //properties.put("bootstrap.servers", "47.98.176.164:9092,47.98.47.81:9092,116.62.119.79:9092");
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
            //Properties properties = new Properties();
            properties = new Properties();
            //properties.put("bootstrap.servers", "172.31.43.13:9092");//xxx是服务器集群的ip
            properties.put("bootstrap.servers", "192.168.61.132:9092");//xxx是服务器集群的ip
            //properties.put("bootstrap.servers", "47.98.176.164:9092");//xxx是服务器集群的ip
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
            BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("D:\\Users\\veev\\BigData\\userBehaviors"))));
            //BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/usr/local/src/userBehaviors"))));
            PreparedStatement preparedStatement;
            int index=0;
            ConsumerRecords<String, String> records;
            String messagetype="";
            String messagebody;
            String[] strArray;
            ResultSet resultSet;
            boolean isExit=false;
            while (true) {
                // records = kafkaConsumer.poll(100);
                records = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));//接收消息
                for (ConsumerRecord<String, String> record : records) {
                    //通信协议格式形如：
                    // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                    String message=record.value();
                    if(message.contains("messagetype"))
                        messagetype=message.substring(message.indexOf("'messagetype'")+14,message.indexOf("}"));//indexOf 方法返回一个整数值，指出 String 对象内子字符串的开始位置。如果没有找到子字符串，则返回-1。
                    if(messagetype.equals("3")){
                        messagebody=message.substring(message.indexOf("}")+1);
                        index++;
                        System.out.println(index);
                        System.out.println(messagebody);
                        //String[] strArray = messagebody.split("\u0001");
                        strArray = messagebody.split("\\s+");//以"\s+"分割字符串

                        //写入文件
                        // 格式："rowkey userid behavior articleid behaviortime degree domain"，字段以"\u0001"分隔
                        writer.write(Integer.toString(index)+"\u0001"+strArray[0]+"\u0001"+strArray[1]+"\u0001"+strArray[2]+"\u0001"+strArray[3]+" "+strArray[4]);
                        //System.out.print(index+"\u0001"+strArray[0]+"\u0001"+strArray[1]+"\u0001"+strArray[2]+"\u0001"+strArray[3]+" "+strArray[4]);

                        preparedStatement = con.prepareStatement("select level,degree from users where userid='"+strArray[0]+"';");
                        resultSet=preparedStatement.executeQuery();
                        if(resultSet.next()) {
                            int level = resultSet.getInt(1);//注意，MySQL API均以1开始，而不是以0开始
                            Object degree = resultSet.getObject(2);//int degree = resultSet.getInt(2);
                            //System.out.println("Degree of user '"+strArray[0]+"': "+degree);

                            //写入文件
                            if(degree!=null)
                                writer.write("\u0001"+Integer.toString(level)+"\u0001"+Integer.toString((int)degree));
                            else
                                writer.write("\u0001"+Integer.toString(level)+"\u0001");
                            //System.out.print("\u0001"+level+"\u0001"+degree);
                        }
                        resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                        preparedStatement = con.prepareStatement("select domain from articles where articleid='"+strArray[2]+"';");
                        resultSet=preparedStatement.executeQuery();
                        if(resultSet.next()) {
                            String domain = resultSet.getString(1);
                            //System.out.println("Domain of article '"+strArray[2]+"': "+domain);

                            //写入文件
                            writer.write("\u0001"+domain);
                            //System.out.print("\u0001"+domain);
                        }
                        resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                        //写入文件，换行
                        writer.write("\n");
                        //System.out.println("");

                        if(index%100000==0)System.gc();
                    }else if(messagetype.equals("-1")){
                        writer.close();
                        System.out.println("Operation finished!");
                        isExit=true;
                        break;
                    }
                }
                if(isExit)break;
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}

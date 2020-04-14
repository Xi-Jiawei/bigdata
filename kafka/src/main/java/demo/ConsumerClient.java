package demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/* articleInfo
D033360700630201
D0568222001010101
D0080328001780201
D046845800760201
D0240524001350111
D073015300640101
D027108600750101
D0435274002410101
D0949726003060101
D0569214001070201
D049515900820101
D0155076002320101
D081729400450201
D0342850002100101
D0671670002640201
D010093300590101*/

/*userSkill
U0000007cassandra,jquery,mongodb,mysql,objectc,plsql,visualbasic,windows,websphere,ios,sybase,cocos2d,android,qt,css,eclipse,hadoop,javascript,matlab,memcache,opengl,kettle,.net,adobeflash,jsp,spring,xml,html,nosql,struts,access,hibernate,linux,lucene,postgresql,visualstudio,delphi,c#,wince,uml,oracle,git,java,extjs,php,python,sqlite,unix,vim,bootstrap,thinkphp,c++,sqlserver,spark,perl,mybatis,mac*/

/*
 * @title: 大数据课程实验二
 * @implement: kafka消费者接收，接收对应类ProducerClient发送过来的数据，并存入cluster2的mysql数据库。数据集来自“Datasets_1”
 * @author: xijiawei
 * @from: 2019.12.23
 * */
public class ConsumerClient {
    public static void main(String[] args) {
        String topic="mykafka";
        AdminClient adminClient;
        Properties properties = new Properties();
        //properties.put("bootstrap.servers", "172.31.43.10:9092,172.31.43.13:9092,172.31.43.18:9092,172.31.43.30:9092");
        //properties.put("bootstrap.servers", "172.31.42.237:9092,172.31.43.12:9092,172.31.43.21:9092");
        properties.put("bootstrap.servers", "192.168.61.130:9092,192.168.61.131:9092,192.168.61.132:9092");
        adminClient = AdminClient.create(properties);
        //删除topic
        try {
            List<String> topics=new ArrayList<String>();
            topics.add(topic);
            adminClient.deleteTopics(topics);
            //adminClient.close();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

        //创建topic
        try {
            NewTopic newTopic = new NewTopic(topic,1,(short) 3);//partitions:1; replication-factor:3
            adminClient.createTopics(Arrays.asList(newTopic));
            //adminClient.close();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

        //连接Kafka，启动消费者
        //Properties properties = new Properties();
        properties = new Properties();
        //properties.put("bootstrap.servers", "172.31.43.13:9092");//xxx是服务器集群的ip
        properties.put("bootstrap.servers", "192.168.61.131:9092");//xxx是服务器集群的ip
        properties.put("group.id", "test-consumer-group");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));

        //声明Connection对象
        Connection con;
        //驱动程序名，加载驱动前要先添加mysql连接器
        String driver = "com.mysql.cj.jdbc.Driver";//String driver = "com.mysql.jdbc.Driver";
        //URL指向要访问的数据库名
        String url = "jdbc:mysql://192.168.61.131:3306/mydb?serverTimezone=Asia/Shanghai";//连接指定数据库前先要创建此数据库："create database if not exists mydb;"。String url = "jdbc:mysql://172.31.43.13:3306/mydb"
        //MySQL配置时的用户名
        String user = "root";
        //MySQL配置时的密码
        String password = "314159";
        //遍历查询结果集
        try {
            Class.forName(driver);//加载驱动程序
            con = DriverManager.getConnection(url, user, password);
            if (!con.isClosed())
                System.out.println("succeeded connecting to the database!");

            PreparedStatement preparedStatement1;
            PreparedStatement preparedStatement2;
            PreparedStatement preparedStatement3;
            Statement createStatement1;
            Statement createStatement2;
            Statement createStatement3;

            //删除旧表，
            // 注意到可能报错“Cannot drop table 'users' referenced by a foreign key constraint 'user_behaviors_ibfk_1' on table 'user_behaviors'.”
            //而无法删除表，所以按顺序先删除user_behaviors表，然后删除users表和articles表
            preparedStatement3= con.prepareStatement("drop table if exists user_behaviors;");
            preparedStatement1= con.prepareStatement("drop table if exists users;");
            preparedStatement2= con.prepareStatement("drop table if exists articles;");
            preparedStatement3.executeUpdate();
            preparedStatement1.executeUpdate();
            preparedStatement2.executeUpdate();

            //创建新表
            createStatement1= con.createStatement();
            createStatement2= con.createStatement();
            createStatement3= con.createStatement();
            createStatement1.executeUpdate("create table if not exists users (" +
                    "userid varchar(100) not null primary key," +
                    "gender integer(1) not null," +
                    "status integer(1) not null," +
                    "follownum integer(10) not null," +
                    "fansnum integer(10) not null," +
                    "friendnum integer(10) not null," +
                    "degree integer(2)," +
                    "school varchar(255)," +
                    "major varchar(255)," +
                    "skill varchar(1000) not null," +
                    "interest varchar(1000) not null);");
            createStatement2.executeUpdate("create table if not exists articles (" +
                    "articleid varchar(100) not null primary key," +
                    "diggcount integer(10) not null," +
                    "viewcount integer(10) not null," +
                    "commentcount integer(10) not null," +
                    "articletype integer(2) not null," +
                    "istop integer(2) not null," +
                    "status integer(2) not null);");
            createStatement3.executeUpdate("create table if not exists user_behaviors (" +
                    "userid varchar(100) not null," +
                    "behavior integer not null," +
                    "articleid varchar(100) not null," +
                    "behaviortime datetime(1) not null," +
                    "foreign key(userid) references users(userid)," +
                    "foreign key(articleid) references articles(articleid));");

            //插入数据
            preparedStatement1 = con.prepareStatement("insert users values(?,?,?,?,?,?,?,?,?,?,?);");
            preparedStatement2 = con.prepareStatement("insert articles values(?,?,?,?,?,?,?);");
            preparedStatement3 = con.prepareStatement("insert user_behaviors values(?,?,?,?);");
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);//接收消息
                for (ConsumerRecord<String, String> record : records) {
                    //通信协议格式形如：
                    // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                    String message=record.value();
                    String messagetype="";
                    //if(message.indexOf("type")!=-1)
                    if(message.contains("type"))
                        messagetype=message.substring(message.indexOf("'messagetype'")+14,message.indexOf("'messagetype'")+15);//indexOf 方法返回一个整数值，指出 String 对象内子字符串的开始位置。如果没有找到子字符串，则返回-1。

                    if(messagetype.equals("1")){
                        String messagebody=message.substring(message.indexOf("}")+1);
                        System.out.println(messagebody);
                        //String[] strArray = messagebody.split("\u0001");
                        String[] strArray = messagebody.split("\\s+");//以空格分割字符串
                        if(strArray[0].equals("U0080233")) {
                            System.out.println("strArray[0]: " + strArray[0]);
                            System.out.println("strArray[1]: " + strArray[1]);
                            System.out.println("strArray[2]: " + strArray[2]);
                            System.out.println("strArray[3]: " + strArray[3]);
                            System.out.println("strArray[4]: " + strArray[4]);
                            System.out.println("strArray[5]: " + strArray[5]);
                            System.out.println("strArray[6]: " + strArray[6]);
                            System.out.println("strArray[7]: " + strArray[7]);
                            System.out.println("strArray[8]: " + strArray[8]);
                            System.out.println("strArray[9]: " + strArray[9]);
                            System.out.println("strArray[10]: " + strArray[10]);
                        }
                        preparedStatement1.setString(1, strArray[0]);
                        preparedStatement1.setInt(2, Integer.parseInt(strArray[1]));
                        preparedStatement1.setInt(3, Integer.parseInt(strArray[2]));
                        preparedStatement1.setInt(4, Integer.parseInt(strArray[3]));
                        preparedStatement1.setInt(5, Integer.parseInt(strArray[4]));
                        preparedStatement1.setInt(6, Integer.parseInt(strArray[5]));
                        if(!strArray[6].equals("\u0001"))preparedStatement1.setInt(7, Integer.parseInt(strArray[6]));
                        if(!strArray[7].equals("\u0001"))preparedStatement1.setString(8, strArray[7]);
                        if(!strArray[8].equals("\u0001"))preparedStatement1.setString(9, strArray[8]);
                        preparedStatement1.setString(10, strArray[9]);
                        preparedStatement1.setString(11, strArray[10]);
                        preparedStatement1.executeUpdate();
                    }else if(messagetype.equals("2")){
                        String messagebody=message.substring(message.indexOf("}")+1);
                        System.out.println(messagebody);
                        //String[] strArray = messagebody.split("\u0001");
                        String[] strArray = messagebody.split("\\s+");//以空格分割字符串
                        preparedStatement2.setString(1, strArray[0]);
                        preparedStatement2.setInt(2, Integer.parseInt(strArray[1]));
                        preparedStatement2.setInt(3, Integer.parseInt(strArray[2]));
                        preparedStatement2.setInt(4, Integer.parseInt(strArray[3]));
                        preparedStatement2.setInt(5, Integer.parseInt(strArray[4]));
                        preparedStatement2.setInt(6, Integer.parseInt(strArray[5]));
                        preparedStatement2.setInt(7, Integer.parseInt(strArray[6]));
                        preparedStatement2.executeUpdate();
                    }else if(messagetype.equals("3")){
                        String messagebody=message.substring(message.indexOf("}")+1);
                        System.out.println(messagebody);
                        //String[] strArray = messagebody.split("\u0001");
                        String[] strArray = messagebody.split("\\s+");//以空格分割字符串
                        String date=strArray[3]+" "+strArray[4];
                        preparedStatement3.setString(1, strArray[0]);
                        preparedStatement3.setInt(2, Integer.parseInt(strArray[1]));
                        preparedStatement3.setString(3, strArray[2]);
                        preparedStatement3.setString(4, date);
                        preparedStatement3.executeUpdate();
                    }
                }
            }
        }catch(ClassNotFoundException e)
        {
            //数据库驱动类异常处理
            System.out.println("sorry,can`t find the Driver!");
            e.printStackTrace();
        }
        catch(SQLException e)
        {
            //数据库连接失败异常处理
            e.printStackTrace();
        }
        catch (Exception e)
        {
            // TODO: handle exception
            e.printStackTrace();
        }
        finally
        {
            System.out.println("successful operation!");
        }
    }
}

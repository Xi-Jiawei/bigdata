package app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.*;

/*
 * @for: experiment 3
 * @author: xijiawei
 * @from: 2019.12.23
 * */
public class KafkaProducerClient {
    public static void main(String[] args) {
        int length;
        //读取数据
        ArrayList<String> userBehaviorList = new ArrayList<String>();
        try {
            File file;
            InputStreamReader inputReader;
            BufferedReader bf;
            String str;

            //读取用户行为数据
            file = new File("D:\\Files\\Documents\\Study\\Curriculums\\研究生\\大数据\\Datasets_3\\数据仓库课程实验三数据\\userBehavior\\userBehavior");
            //file = new File("/usr/local/src/Datasets_3/userBehavior/userBehavior");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                userBehaviorList.add(str);
            }

            bf.close();
            inputReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //用户行为数据
        // 分割字符串
        List<UserBehavior> list=new ArrayList<UserBehavior>();
        length = userBehaviorList.size();
        for (int i = 0; i < length; i++) {
            //String[] strArray = arrayList.get(i).split("\\s+");//arrayList.get(i).split(" ");
            String[] strArray = userBehaviorList.get(i).split("\u0001");
            UserBehavior userBehavior=new UserBehavior(strArray[0],strArray[1],strArray[2],strArray[3]);
            list.add(userBehavior);
        }
        userBehaviorList=null;
        System.gc();//手动释放内存
        // 按照行为时间排列
        Collections.sort(list, new Comparator<UserBehavior>() {
            public int compare(UserBehavior o1, UserBehavior o2) {
                // 按照行为时间进行升序排列
                if (o1.getBehaviortime().compareTo(o2.getBehaviortime())>0) {
                    return 1;
                }
                if (o1.getBehaviortime().compareTo(o2.getBehaviortime())<0) {
                    return -1;
                }else return 0;
            }
        });
        //合并字符串
        final ArrayList<String> userBehaviorDataList = new ArrayList<String>();
        for (UserBehavior userBehavior:list)
            userBehaviorDataList.add(userBehavior.getUserid()+" "+userBehavior.getBehavior()+" "+userBehavior.getArticleid()+" "+userBehavior.getBehaviortime());

        final String topic="mykafka";
        //final String server_ip="172.31.43.12:9092";
        final String server_ip="192.168.61.130:9092";
        //final String server_ip="116.62.119.79:9092";
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", server_ip);//xxx服务器ip
        properties.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        properties.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题:)
        properties.put("batch.size", 16384);//producer将试图批处理消息记录，以减少请求次数.默认的批量处理消息字节数
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        properties.put("linger.ms", 1);//延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        properties.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //发送用户行为数据
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);//连接Kafka中的指定话题
        try {
            length = userBehaviorDataList.size();
            for (int i = 0; i < length; i ++) {
                //通信协议格式形如：
                // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                //messagetype=1表示用户信息数据；messagetype=2表示博客信息数据；messagetype=3表示用户行为数据
                String message = "{'serverip':" + server_ip + ",'messagetype':3}" + userBehaviorDataList.get(i);
                kafkaProducer.send(new ProducerRecord<String, String>("mykafka", message));
                System.out.println(message);
            }
            kafkaProducer.send(new ProducerRecord<String, String>("mykafka", "{'serverip':" + server_ip + ",'messagetype':-1}"));//messagetype:-1，结束标志
            System.out.println("{'serverip':" + server_ip + ",'messagetype':-1}");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}

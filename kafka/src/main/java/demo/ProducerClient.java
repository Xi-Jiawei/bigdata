package demo;

import app.UserBehavior;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.*;

/*
* @title: 大数据课程实验二
* @implement: kafka生产者发送数据，对应类ConsumerClient接收数据。数据集来自“Datasets_1”
* @author: xijiawei
* @from: 2019.12.23
* */
public class ProducerClient {
    public static void main(String[] args) {
        int length;
        //读取数据
        ArrayList<String> userBasicList = new ArrayList<String>();
        ArrayList<String> userEduList = new ArrayList<String>();
        ArrayList<String> userSkillList = new ArrayList<String>();
        ArrayList<String> userInterestList = new ArrayList<String>();
        ArrayList<String> articleInfoList = new ArrayList<String>();
        ArrayList<String> userBehaviorList = new ArrayList<String>();
        try {
            File file;
            InputStreamReader inputReader;
            BufferedReader bf;
            String str;

            //读取用户信息数据
            file = new File("D:\\Files\\Documents\\Study\\Courseware\\研究生\\大数据\\Datasets_1\\数据仓库实验数据\\某些数据\\userBasic\\userBasic");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            // 按行读取字符串
            while ((str = bf.readLine()) != null) {
                userBasicList.add(str);
            }
            file = new File("D:\\Files\\Documents\\Study\\Courseware\\研究生\\大数据\\Datasets_1\\数据仓库实验数据\\某些数据\\userEdu\\userEdu");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                userEduList.add(str);
            }
            file = new File("D:\\Files\\Documents\\Study\\Courseware\\研究生\\大数据\\Datasets_1\\数据仓库实验数据\\某些数据\\userSkill\\userSkill");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                userSkillList.add(str);
            }
            file = new File("D:\\Files\\Documents\\Study\\Courseware\\研究生\\大数据\\Datasets_1\\数据仓库实验数据\\某些数据\\userInterest\\userInterest");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                userInterestList.add(str);
            }

            //读取博客信息数据
            file = new File("D:\\Files\\Documents\\Study\\Courseware\\研究生\\大数据\\Datasets_1\\数据仓库实验数据\\某些数据\\articleInfo\\articleInfo");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                articleInfoList.add(str);
            }

            //读取用户行为数据
            file = new File("D:\\Files\\Documents\\Study\\Courseware\\研究生\\大数据\\Datasets_1\\数据仓库实验数据\\某些数据\\userBehavior\\userBehavior");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                userBehaviorList.add(str);
            }

            bf.close();
            inputReader.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        /* 整合用户信息数据
        * userEdu部分信息为空格的特殊情况：
        * 1、uid 方格 方格 方格
        * 2、uid 方格 学历 方格 空格 方格 专业
        * 因为整合后的字符串用空格隔开，在整合时如果某项信息为空格，则分割处理时会出现困难，
        * 所以要判断：
        * 1、userEduArray的长度
        * 2、userEduArray的元素是否为空格，如果为空则将其替换成方格
        * */
        final ArrayList<String> userInfoDataList = new ArrayList<String>();
        length = userBasicList.size();
        for (int i = 0; i < length; i++) {
            String[] userBasicArray = userBasicList.get(i).split("\u0001");
            String[] userEduArray = userEduList.get(i).split("\u0001");
            String[] userSkillArray = userSkillList.get(i).split("\u0001");
            String[] userInterestArray = userInterestList.get(i).split("\u0001");
            if(userBasicArray[0].equals("U0080233")) {
                System.out.println("userEduArray[1]: "+userEduArray[1]);
                System.out.println("userEduArray[2]: "+userEduArray[2]);
                System.out.println("userEduArray[3]: "+userEduArray[3]);
            }
            if(userEduArray.length>3) {
                for(int j=1;j<=3;j++)
                    if(userEduArray[j].trim().isEmpty())
                        userEduArray[j]="\u0001";
                userInfoDataList.add(userBasicArray[0] + " " +
                        userBasicArray[1] + " " +
                        userBasicArray[2] + " " +
                        userBasicArray[3] + " " +
                        userBasicArray[4] + " " +
                        userBasicArray[5] + " " +
                        userEduArray[1] + " " +
                        userEduArray[2] + " " +
                        userEduArray[3] + " " +
                        userSkillArray[1] + " " +
                        userInterestArray[1]);
            }
            else if(userEduArray.length>2){
                for(int j=1;j<=2;j++)
                    if(userEduArray[j].trim().isEmpty())
                        userEduArray[j]="\u0001";
                userInfoDataList.add(userBasicArray[0]+" "+
                        userBasicArray[1]+" "+
                        userBasicArray[2]+" "+
                        userBasicArray[3]+" "+
                        userBasicArray[4]+" "+
                        userBasicArray[5]+" "+
                        userEduArray[1]+" "+
                        userEduArray[2]+" "+
                        "\u0001"+" "+
                        userSkillArray[1]+" "+
                        userInterestArray[1]);
            }
            else if(userEduArray.length>1) {
                if (userEduArray[1].trim().isEmpty()) userEduArray[1] = "\u0001";
                userInfoDataList.add(userBasicArray[0] + " " +
                        userBasicArray[1] + " " +
                        userBasicArray[2] + " " +
                        userBasicArray[3] + " " +
                        userBasicArray[4] + " " +
                        userBasicArray[5] + " " +
                        userEduArray[1] + " " +
                        "\u0001" + " " +
                        "\u0001" + " " +
                        userSkillArray[1] + " " +
                        userInterestArray[1]);
            }
            else
                userInfoDataList.add(userBasicArray[0]+" "+
                        userBasicArray[1]+" "+
                        userBasicArray[2]+" "+
                        userBasicArray[3]+" "+
                        userBasicArray[4]+" "+
                        userBasicArray[5]+" "+
                        "\u0001"+" "+
                        "\u0001"+" "+
                        "\u0001"+" "+
                        userSkillArray[1]+" "+
                        userInterestArray[1]);
        }

        //博客信息数据
        final ArrayList<String> articleInfoDataList = new ArrayList<String>();
        length = articleInfoList.size();
        for (int i = 0; i < length; i++) {
            String[] strArray = articleInfoList.get(i).split("\u0001");
            articleInfoDataList.add(strArray[0]+" "+strArray[1]+" "+strArray[2]+" "+strArray[3]+" "+strArray[4]+" "+strArray[5]+" "+strArray[6]+" "+strArray[7]);
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
        userBehaviorList=null;System.gc();//手动释放内存
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
        final String server_ip="192.168.14.130:9092";
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

        try {
            //线程池运行3个线程
            /*ExecutorService executorService = Executors.newFixedThreadPool(10);
            //发送用户信息数据
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        //lock.lock();
                        System.out.println(Thread.currentThread().getName());

                        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);//连接Kafka中的指定话题
                        int length = userInfoDataList.size();
                        for (int i = 0; i < length; i ++) {
                            //通信协议格式形如：
                            // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                            //messagetype=1表示用户信息数据；messagetype=2表示博客信息数据；messagetype=3表示用户行为数据
                            String message = "{'serverip':" + server_ip + ",'messagetype':1}" + userInfoDataList.get(i);
                            kafkaProducer.send(new ProducerRecord<String, String>("mykafka", message));
                            System.out.println(Thread.currentThread().getName() + ": " + message);
                        }
                    }catch (Exception e){
                        // TODO: handle exception
                    }finally {
                        //lock.unlock();
                    }
                }
            });
            //发送博客信息数据
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        //lock.lock();
                        System.out.println(Thread.currentThread().getName());

                        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);//连接Kafka中的指定话题
                        int length = articleInfoDataList.size();
                        for (int i = 0; i < length; i ++) {
                            //通信协议格式形如：
                            // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                            //messagetype=1表示用户信息数据；messagetype=2表示博客信息数据；messagetype=3表示用户行为数据
                            String message = "{'serverip':" + server_ip + ",'messagetype':2}" + articleInfoDataList.get(i);
                            kafkaProducer.send(new ProducerRecord<String, String>("mykafka", message));
                            System.out.println(Thread.currentThread().getName() + ": " + message);
                        }
                    }catch (Exception e){
                        // TODO: handle exception
                    }finally {
                        //lock.unlock();
                    }
                }
            });
            //发送用户行为数据
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        //lock.lock();
                        System.out.println(Thread.currentThread().getName());

                        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);//连接Kafka中的指定话题
                        int length = userBehaviorDataList.size();
                        for (int i = 0; i < length; i ++) {
                            //通信协议格式形如：
                            // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                            //messagetype=1表示用户信息数据；messagetype=2表示博客信息数据；messagetype=3表示用户行为数据
                            String message = "{'serverip':" + server_ip + ",'messagetype':3}" + userBehaviorDataList.get(i);
                            kafkaProducer.send(new ProducerRecord<String, String>("mykafka", message));
                            System.out.println(Thread.currentThread().getName() + ": " + message);
                        }
                    }catch (Exception e){
                        // TODO: handle exception
                    }finally {
                        //lock.unlock();
                    }
                }
            });
            */
            /*Thread thread=new Thread() {
                @Override
                public void run() {
                    try {
                        //lock.lock();
                        System.out.println(Thread.currentThread().getName());
                        // TODO: action
                    }catch (Exception e){
                        // TODO: handle exception
                    }finally {
                        //lock.unlock();
                    }
                }
            };
            thread.start();*/

            //先发送用户信息，博客信息，最后发送用户行为数据
            KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);//连接Kafka中的指定话题
            length = userInfoDataList.size();
            for (int i = 0; i < length; i ++) {
                //通信协议格式形如：
                // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                //messagetype=1表示用户信息数据；messagetype=2表示博客信息数据；messagetype=3表示用户行为数据
                String message = "{'serverip':" + server_ip + ",'messagetype':1}" + userInfoDataList.get(i);
                kafkaProducer.send(new ProducerRecord<String, String>("mykafka", message));
                System.out.println(message);
            }

            length = articleInfoDataList.size();
            for (int i = 0; i < length; i ++) {
                //通信协议格式形如：
                // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                //messagetype=1表示用户信息数据；messagetype=2表示博客信息数据；messagetype=3表示用户行为数据
                String message = "{'serverip':" + server_ip + ",'messagetype':2}" + articleInfoDataList.get(i);
                kafkaProducer.send(new ProducerRecord<String, String>("mykafka", message));
                System.out.println(message);
            }

            length = userBehaviorDataList.size();
            for (int i = 0; i < length; i ++) {
                //通信协议格式形如：
                // {'serverip':172.31.42.237:9092,'messagetype':1}messagebody;
                //messagetype=1表示用户信息数据；messagetype=2表示博客信息数据；messagetype=3表示用户行为数据
                String message = "{'serverip':" + server_ip + ",'messagetype':3}" + userBehaviorDataList.get(i);
                kafkaProducer.send(new ProducerRecord<String, String>("mykafka", message));
                System.out.println(message);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //kafkaProducer.close();
        }
    }

    public ArrayList<String> Integrate_deprecated1(ArrayList<String> userBasicList,ArrayList<String> userEduList,ArrayList<String> userSkillList,ArrayList<String> userInterestList){
        //整合用户信息数据
        List<app.UserInfo> userInfoList=new ArrayList<app.UserInfo>();
        int length = userBasicList.size();
        for (int i = 0; i < length; i++) {
            String[] strArray = userBasicList.get(i).split("\u0001");
            app.UserInfo userInfo=new app.UserInfo(strArray[0],strArray[1],strArray[2],strArray[3],strArray[4],strArray[5]);
            userInfoList.add(userInfo);
        }
        length = userEduList.size();
        for (int i = 0; i < length; i++) {
            String[] strArray = userEduList.get(i).split("\u0001");
            for (app.UserInfo userInfo:userInfoList)
                if(strArray[0].equals(userInfo.getUserid())) {
                    if(strArray.length>1)userInfo.setDgreee(strArray[1]);
                    else {userInfo.setDgreee("\u0001");userInfo.setSchool("\u0001");userInfo.setMajor("\u0001");}
                    if(strArray.length>2)userInfo.setSchool(strArray[2]);
                    else {userInfo.setSchool("\u0001");userInfo.setMajor("\u0001");}
                    if(strArray.length>3)userInfo.setMajor(strArray[3]);
                    else userInfo.setMajor("\u0001");
                    break;
                }
        }
        length = userSkillList.size();
        for (int i = 0; i < length; i++) {
            String[] strArray = userSkillList.get(i).split("\u0001");
            for (app.UserInfo userInfo:userInfoList)
                if(strArray[0].equals(userInfo.getUserid())) {
                    userInfo.setDgreee(strArray[1]);
                    break;
                }
        }
        length = userInterestList.size();
        for (int i = 0; i < length; i++) {
            String[] strArray = userInterestList.get(i).split("\u0001");
            for (app.UserInfo userInfo:userInfoList)
                if(strArray[0].equals(userInfo.getUserid())) {
                    userInfo.setDgreee(strArray[1]);
                    break;
                }
        }

        //合并字符串
       /* ArrayList<String> userInfoDataList = new ArrayList<>();
        for (app.UserInfo userInfo:userInfoList)
            userInfoDataList.add(userInfo.getUserid()+" "+
                    userInfo.getGender()+" "+
                    userInfo.getStatus()+" "+
                    userInfo.getFollownum()+" "+
                    userInfo.getFansnum()+" "+
                    userInfo.getFriendnum()+" "+
                    userInfo.getDgreee()+" "+
                    userInfo.getSchool()+" "+
                    userInfo.getMajor()+" "+
                    userInfo.getSkill()+" "+
                    userInfo.getInterest());*/
        ArrayList<String> userInfoDataList = new ArrayList<>();
        length = userBasicList.size();
        for (int i = 0; i < length; i++) {
            String[] userBasicArray = userBasicList.get(i).split("\u0001");
            String[] userEduArray = userEduList.get(i).split("\u0001");
            String[] userSkillArray = userSkillList.get(i).split("\u0001");
            String[] userInterestArray = userInterestList.get(i).split("\u0001");
            if(userEduArray.length>1)
                userInfoDataList.add(userBasicArray[0]+"\u0001"+
                        userBasicArray[1]+"\u0001"+
                        userBasicArray[2]+"\u0001"+
                        userBasicArray[3]+"\u0001"+
                        userBasicArray[4]+"\u0001"+
                        userBasicArray[5]+"\u0001"+
                        userEduArray[1]+"\u0001"+
                        userEduArray[2]+"\u0001"+
                        userEduArray[3]+"\u0001"+
                        userSkillArray[1]+"\u0001"+
                        userInterestArray[1]);
            else
                userInfoDataList.add(userBasicArray[0]+"\u0001"+
                        userBasicArray[1]+"\u0001"+
                        userBasicArray[2]+"\u0001"+
                        userBasicArray[3]+"\u0001"+
                        userBasicArray[4]+"\u0001"+
                        userBasicArray[5]+"\u0001"+
                        " "+"\u0001"+
                        " "+"\u0001"+
                        " "+"\u0001"+
                        userSkillArray[1]+"\u0001"+
                        userInterestArray[1]);
        }

        return userInfoDataList;
    }

    public ArrayList<String> Integrate_deprecated2(ArrayList<String> userBasicList,ArrayList<String> userEduList,ArrayList<String> userSkillList,ArrayList<String> userInterestList){
        final ArrayList<String> userInfoDataList = new ArrayList<String>();
        int length = userBasicList.size();
        for (int i = 0; i < length; i++) {
            String[] userBasicArray = userBasicList.get(i).split("\u0001");
            String[] userEduArray = userEduList.get(i).split("\u0001");
            String[] userSkillArray = userSkillList.get(i).split("\u0001");
            String[] userInterestArray = userInterestList.get(i).split("\u0001");
            if(userBasicArray[0].equals("U0080233")) {
                System.out.println("userEduArray[1]: "+userEduArray[1]);
                System.out.println("userEduArray[2]: "+userEduArray[2]);
                System.out.println("userEduArray[3]: "+userEduArray[3]);
            }
            if(userEduArray.length>3)
                userInfoDataList.add(userBasicArray[0]+" "+
                        userBasicArray[1]+" "+
                        userBasicArray[2]+" "+
                        userBasicArray[3]+" "+
                        userBasicArray[4]+" "+
                        userBasicArray[5]+" "+
                        userEduArray[1]+" "+
                        userEduArray[2]+" "+
                        userEduArray[3]+" "+
                        userSkillArray[1]+" "+
                        userInterestArray[1]);
            else if(userEduArray.length>2)
                userInfoDataList.add(userBasicArray[0]+" "+
                        userBasicArray[1]+" "+
                        userBasicArray[2]+" "+
                        userBasicArray[3]+" "+
                        userBasicArray[4]+" "+
                        userBasicArray[5]+" "+
                        userEduArray[1]+" "+
                        userEduArray[2]+" "+
                        "\u0001"+" "+
                        userSkillArray[1]+" "+
                        userInterestArray[1]);
            else if(userEduArray.length>1)
                userInfoDataList.add(userBasicArray[0]+" "+
                        userBasicArray[1]+" "+
                        userBasicArray[2]+" "+
                        userBasicArray[3]+" "+
                        userBasicArray[4]+" "+
                        userBasicArray[5]+" "+
                        userEduArray[1]+" "+
                        "\u0001"+" "+
                        "\u0001"+" "+
                        userSkillArray[1]+" "+
                        userInterestArray[1]);
            else
                userInfoDataList.add(userBasicArray[0]+" "+
                        userBasicArray[1]+" "+
                        userBasicArray[2]+" "+
                        userBasicArray[3]+" "+
                        userBasicArray[4]+" "+
                        userBasicArray[5]+" "+
                        "\u0001"+" "+
                        "\u0001"+" "+
                        "\u0001"+" "+
                        userSkillArray[1]+" "+
                        userInterestArray[1]);
        }

        return userInfoDataList;
    }
}

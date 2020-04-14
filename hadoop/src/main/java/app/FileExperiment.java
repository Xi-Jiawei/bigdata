package app;

import java.io.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class FileExperiment {
    public static void main(String[] args) throws Throwable {
        try {
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

            /*读取数据，并插入到HBase*/
            BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("D:\\Users\\veev\\BigData\\userBehavior"))));
            //FileWriter writer=new FileWriter("D:\\Users\\veev\\BigData\\userBehaviors");
            PreparedStatement preparedStatement;
            ResultSet resultSet;
            int index=0;
            //读取用户行为数据
            List<UserBehavior> userBehaviorList=new ArrayList<UserBehavior>();
            String line;
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("D:\\Files\\Documents\\Study\\Curriculums\\研究生\\大数据\\Datasets_3\\数据仓库课程实验三数据\\userBehavior\\userBehavior"))),8*1024*1024);
            //BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/usr/local/src/Datasets_3/userBehavior/userBehavior"))));
            /*while ((line = reader.readLine()) != null) {
                index++;
                System.out.println("index: "+index);
                System.out.println(line);
                String[] strArray = line.split("\u0001");//以"\u0001"分割字符串

                // 格式："rowkey userid behavior articleid behaviortime degree domain"，字段以"\u0001"分隔
                writer.write(Integer.toString(index)+"\u0001"+strArray[0]+"\u0001"+strArray[1]+"\u0001"+strArray[2]+"\u0001"+strArray[3]);
                //writer.append(Integer.toString(index)+"\u0001"+strArray[0]+"\u0001"+strArray[1]+"\u0001"+strArray[2]+"\u0001"+strArray[3]);

                preparedStatement = con.prepareStatement("select level,degree from users where userid='"+strArray[0]+"';");
                resultSet=preparedStatement.executeQuery();
                if(resultSet.next()) {
                    int level = resultSet.getInt(1);//注意，MySQL API均以1开始，而不是以0开始
                    Object degree = resultSet.getObject(2);//int degree = resultSet.getInt(2);
                    System.out.println("Degree of user '"+strArray[0]+"': "+degree);

                    //写入文件
                    if(degree!=null)
                        writer.write("\u0001"+Integer.toString(level)+"\u0001"+Integer.toString((Integer)degree));
                        //writer.append("\u0001"+Integer.toString(level)+"\u0001"+Integer.toString((Integer)degree));
                    else
                        writer.write("\u0001"+Integer.toString(level)+"\u0001");
                        //writer.append("\u0001"+Integer.toString(level)+"\u0001");
                }
                resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                preparedStatement = con.prepareStatement("select domain from articles where articleid='"+strArray[2]+"';");
                resultSet=preparedStatement.executeQuery();
                if(resultSet.next()){
                    String domain = resultSet.getString(1);
                    //System.out.println("Domain of article '"+strArray[2]+"': "+domain);

                    //写入文件
                    writer.write("\u0001"+domain);
                    //writer.append("\u0001"+domain+"\n");
                }
                resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                //写入文件，换行
                writer.write("\n");

                //if(index%10000==0)
                writer.flush();
                if(index%100000==0) {
                    writer.close();
                    System.gc();
                    Thread.sleep(100000);
                    writer=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("D:\\Users\\veev\\BigData\\userBehaviors"),true)),8*1024*1024);
                    //writer=new FileWriter("D:\\Users\\veev\\BigData\\userBehaviors",true);
                }
            }
            reader.close();*/
            while ((line = reader.readLine()) != null) {
                String[] strArray = line.split("\u0001");//以"\u0001"分割字符串
                UserBehavior userBehavior=new UserBehavior(strArray[0],strArray[1],strArray[2],strArray[3]);
                userBehaviorList.add(userBehavior);
            }
            reader.close();
            Collections.sort(userBehaviorList, new Comparator<UserBehavior>() {
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
            for(UserBehavior userBehavior:userBehaviorList) {
                index++;
                System.out.println("index: "+index);
                System.out.println(userBehavior.getUserid()+"\u0001"+userBehavior.getBehavior()+"\u0001"+userBehavior.getArticleid()+"\u0001"+userBehavior.getBehaviortime());

                // 格式："rowkey userid behavior articleid behaviortime degree domain"，字段以"\u0001"分隔
                writer.write(Integer.toString(index)+"\u0001"+userBehavior.getUserid()+"\u0001"+userBehavior.getBehavior()+"\u0001"+userBehavior.getArticleid()+"\u0001"+userBehavior.getBehaviortime());
                //writer.append(Integer.toString(index)+"\u0001"+strArray[0]+"\u0001"+strArray[1]+"\u0001"+strArray[2]+"\u0001"+strArray[3]);

                preparedStatement = con.prepareStatement("select level,degree from users where userid='"+userBehavior.getUserid()+"';");
                resultSet=preparedStatement.executeQuery();
                if(resultSet.next()) {
                    int level = resultSet.getInt(1);//注意，MySQL API均以1开始，而不是以0开始
                    Object degree = resultSet.getObject(2);//int degree = resultSet.getInt(2);
                    System.out.println("Degree of user '"+userBehavior.getUserid()+"': "+degree);

                    //写入文件
                    if(degree!=null)
                        writer.write("\u0001"+Integer.toString(level)+"\u0001"+Integer.toString((int)degree));
                        //writer.append("\u0001"+Integer.toString(level)+"\u0001"+Integer.toString((Integer)degree));
                    else
                        writer.write("\u0001"+Integer.toString(level)+"\u0001");
                    //writer.append("\u0001"+Integer.toString(level)+"\u0001");
                }
                resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                preparedStatement = con.prepareStatement("select domain from articles where articleid='"+userBehavior.getArticleid()+"';");
                resultSet=preparedStatement.executeQuery();
                if(resultSet.next()){
                    String domain = resultSet.getString(1);
                    //System.out.println("Domain of article '"+strArray[2]+"': "+domain);

                    //写入文件
                    writer.write("\u0001"+domain);
                    //writer.append("\u0001"+domain+"\n");
                }
                resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                //写入文件，换行
                writer.write("\n");

                //if(index%10000==0)
                writer.flush();
                if(index%100000==0) {
                    writer.close();
                    //System.gc();
                    Thread.sleep(10000);
                    writer=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("D:\\Users\\veev\\BigData\\userBehaviors"),true)),8*1024*1024);
                    //writer=new FileWriter("D:\\Users\\veev\\BigData\\userBehaviors",true);
                }
            }
            writer.close();
            System.out.println("Operation finished!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

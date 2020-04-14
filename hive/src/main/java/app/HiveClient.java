package app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.*;

public class HiveClient {
    public static void main(String[] args) throws Throwable {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            //Connection con = DriverManager.getConnection("jdbc:hive2://192.168.61.132:10000/default", "hadoop", "314159");
            //Connection con = DriverManager.getConnection("jdbc:hive2://47.98.176.164:10000/default", "hadoop", "fionasit61L");
            Connection con = DriverManager.getConnection("jdbc:hive2://116.62.119.79:10000/default", "hadoop", "fionasit61L");
            if (!con.isClosed())
                System.out.println("succeeded connecting to the database!");

            /*删除旧表，创建新表*/
            /*注意：HiveQL的语法规则要比其他数据库要更严格，因为jdbc会为sql语句末尾自动加上分号，所以操作HiveQL时不要加分号*/
            /*注意：Hive表没有主键*/
            Statement createStatement = con.createStatement();
            createStatement.executeUpdate("drop table if exists userBehaviorsCount");
            createStatement.executeUpdate("create table if not exists userBehaviorsCount (" +
                    "behaviortime string," +
                    "level0 int," +
                    "level1 int," +
                    "level2 int," +
                    "level3 int," +
                    "degreeNeg15 int," +
                    "degreeNeg10 int," +
                    "degreeNeg5 int," +
                    "degree0 int," +
                    "degree1 int," +
                    "degree2 int," +
                    "degree3 int," +
                    "degree4 int," +
                    "degree5 int," +
                    "degree6 int," +
                    "degree7 int," +
                    "behavior0 int," +
                    "behavior1 int," +
                    "behavior2 int)");

            /*插入*/
            /*MySQL插入语句可以省略“into”，但是HiveQL不能省略*/
            /*
            insert into userBehaviorsCount values('2015-01-01 00:00:00', 32, 50, 6, 23, 0, 0, 0, 0, 0, 9, 39, 10, 0, 0, 0, 35, 70, 6);
            */
            PreparedStatement preparedStatement;
            preparedStatement = con.prepareStatement("insert into userBehaviorsCount values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            String line;
            //BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("D:\\Users\\veev\\BigData\\userBehaviorsCount"))),8*1024*1024);
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/usr/local/src/userBehaviorsCount"))));
            while ((line = reader.readLine()) != null) {
                String[] strArray = line.split("\u0001");//以"\u0001"分割字符串
                preparedStatement.setString(1, strArray[0]);
                preparedStatement.setInt(2, Integer.parseInt(strArray[1]));
                preparedStatement.setInt(3, Integer.parseInt(strArray[2]));
                preparedStatement.setInt(4, Integer.parseInt(strArray[3]));
                preparedStatement.setInt(5, Integer.parseInt(strArray[4]));
                preparedStatement.setInt(6, Integer.parseInt(strArray[5]));
                preparedStatement.setInt(7, Integer.parseInt(strArray[6]));
                preparedStatement.setInt(8, Integer.parseInt(strArray[7]));
                preparedStatement.setInt(9, Integer.parseInt(strArray[8]));
                preparedStatement.setInt(10, Integer.parseInt(strArray[9]));
                preparedStatement.setInt(11, Integer.parseInt(strArray[10]));
                preparedStatement.setInt(12, Integer.parseInt(strArray[11]));
                preparedStatement.setInt(13, Integer.parseInt(strArray[12]));
                preparedStatement.setInt(14, Integer.parseInt(strArray[13]));
                preparedStatement.setInt(15, Integer.parseInt(strArray[14]));
                preparedStatement.setInt(16, Integer.parseInt(strArray[15]));
                preparedStatement.setInt(17, Integer.parseInt(strArray[16]));
                preparedStatement.setInt(18, Integer.parseInt(strArray[17]));
                preparedStatement.setInt(19, Integer.parseInt(strArray[18]));
                preparedStatement.executeUpdate();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

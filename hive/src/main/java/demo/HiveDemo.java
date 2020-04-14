package demo;

import java.sql.*;

public class HiveDemo {
    public static void main(String[] args){
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
            /*createStatement.executeUpdate("drop table if exists users");
            createStatement.executeUpdate("create table if not exists users (" +
                    "id int," +
                    "name string)");*/

            /*插入*/
            /*MySQL插入语句可以省略“into”，但是HiveQL不能省略*/
            PreparedStatement preparedStatement;
            preparedStatement = con.prepareStatement("insert into users values(?,?)");
            //preparedStatement.setInt(1, 1);
            //preparedStatement.setString(2, "ly");
            preparedStatement.setInt(1, 2);
            preparedStatement.setString(2, "xjw");
            preparedStatement.executeUpdate();

            /*查询*/
            //ResultSet resultSet = con.prepareStatement("select * from users").executeQuery();
            ResultSet resultSet = createStatement.executeQuery("select * from users");
            while (resultSet.next()) {
                System.out.print(resultSet.getInt(1)+" ");
                System.out.println(resultSet.getString(2));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

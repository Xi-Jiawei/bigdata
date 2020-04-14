package demo;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;

public class MySQLDemo {
    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");//"com.mysql.jdbc.Driver"
            Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?serverTimezone=Asia/Shanghai", "root", "123456");
            if (!con.isClosed())
                System.out.println("succeeded connecting to the database!");

            /*删除旧表，创建新表*/
            /*注意：因为jdbc会为sql语句末尾自动加上分号，所以操作MySQL时可以不加分号*/
            Statement createStatement = con.createStatement();
            createStatement.executeUpdate("drop table if exists users");
            createStatement.executeUpdate("create table if not exists users (" +
                    "userid integer not null primary key auto_increment," +
                    "username varchar(50),"+
                    "password varchar(50),"+
                    "sex integer(1),"+
                    "age integer(3),"+
                    "home varchar(50),"+
                    "profile varchar(100))");

            /*插入*/
            /*MySQL插入语句可以省略“into”*/
            PreparedStatement preparedStatement;
            preparedStatement = con.prepareStatement("insert into users values(?,?,?,?,?,?,?)");
            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "liuyan");
            preparedStatement.setString(3, "1102");
            preparedStatement.setInt(4, 0);
            preparedStatement.setObject(5, null);//preparedStatement.setInt(5, 26);
            preparedStatement.setString(6, "成都");
            preparedStatement.setString(7, "61");
            preparedStatement.executeUpdate();
            preparedStatement = con.prepareStatement("insert into users(userid,username,password,sex,home) values(?,?,?,?,?)");
            preparedStatement.setInt(1, 2);
            preparedStatement.setString(2, "xijiawei");
            preparedStatement.setString(3, "0806");
            preparedStatement.setInt(4, 1);
            preparedStatement.setString(5, "吉安");
            preparedStatement.executeUpdate();

            /*查询*/
            //ResultSet resultSet = con.prepareStatement("select * from users").executeQuery();
            ResultSet resultSet = createStatement.executeQuery("select * from users");
            while (resultSet.next()) {
                System.out.print(resultSet.getInt(1)+" ");
                System.out.print(resultSet.getString(2)+" ");
                System.out.print(resultSet.getString(3)+" ");
                System.out.print(resultSet.getInt(4)+" ");
                /*注意：用jdbc的接口函数getInt()查询int字段或用getDouble()查询double字段时，如果为空值，会返回0，而不是null*/
                Object o=resultSet.getObject(5);//System.out.print(resultSet.getInt(5)+" ");
                if(o!=null)
                    System.out.print((int)o+" ");
                else System.out.print(resultSet.getObject(5)+" ");
                System.out.print(resultSet.getString(6)+" ");
                System.out.println(resultSet.getString(7));
            }
        }catch (Exception e)
        {
            // TODO: handle exception
            e.printStackTrace();
        }finally {
            System.out.println("Operation finished!");
        }
    }
}

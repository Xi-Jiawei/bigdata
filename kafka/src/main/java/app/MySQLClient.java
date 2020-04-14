package app;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;

public class MySQLClient {
    public static void main(String[] args) {
        int length;
        //读取数据
        ArrayList<String> userBasicList = new ArrayList<String>();
        ArrayList<String> userEduList = new ArrayList<String>();
        ArrayList<String> userSkillList = new ArrayList<String>();
        ArrayList<String> userInterestList = new ArrayList<String>();
        ArrayList<String> articleInfoList = new ArrayList<String>();
        try {
            File file;
            InputStreamReader inputReader;
            BufferedReader bf;
            String str;

            //读取用户信息数据
            //file = new File("D:\\Files\\Documents\\Study\\Curriculums\\研究生\\大数据\\Datasets_3\\数据仓库课程实验三数据\\userBasic\\userBasic");
            file = new File("/usr/local/src/Datasets_3/userBasic/userBasic");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            // 按行读取字符串
            while ((str = bf.readLine()) != null) {
                userBasicList.add(str);
            }
            //file = new File("D:\\Files\\Documents\\Study\\Curriculums\\研究生\\大数据\\Datasets_3\\数据仓库课程实验三数据\\userEdu\\userEdu");
            file = new File("/usr/local/src/Datasets_3/userEdu/userEdu");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                userEduList.add(str);
            }
            //file = new File("D:\\Files\\Documents\\Study\\Curriculums\\研究生\\大数据\\Datasets_3\\数据仓库课程实验三数据\\userSkill\\userSkill");
            file = new File("/usr/local/src/Datasets_3/userSkill/userSkill");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                userSkillList.add(str);
            }
            //file = new File("D:\\Files\\Documents\\Study\\Curriculums\\研究生\\大数据\\Datasets_3\\数据仓库课程实验三数据\\userInterest\\userInterest");
            file = new File("/usr/local/src/Datasets_3/userInterest/userInterest");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                userInterestList.add(str);
            }

            //读取博客信息数据
            //file = new File("D:\\Files\\Documents\\Study\\Curriculums\\研究生\\大数据\\Datasets_3\\数据仓库课程实验三数据\\articleInfo\\articleInfo");
            file = new File("/usr/local/src/Datasets_3/articleInfo/articleInfo");
            inputReader = new InputStreamReader(new FileInputStream(file));
            bf = new BufferedReader(inputReader);
            while ((str = bf.readLine()) != null) {
                articleInfoList.add(str);
            }

            bf.close();
            inputReader.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        //声明Connection对象
        Connection con;
        //驱动程序名，加载驱动前要先添加mysql连接器
        String driver = "com.mysql.cj.jdbc.Driver";//String driver = "com.mysql.jdbc.Driver";
        //URL指向要访问的数据库名
        //String url = "jdbc:mysql://192.168.61.131:3306/mydb?serverTimezone=Asia/Shanghai";//连接指定数据库前先要创建此数据库："create database if not exists mydb;"。String url = "jdbc:mysql://172.31.43.13:3306/mydb"
        String url = "jdbc:mysql://47.98.47.81:3306/mydb?serverTimezone=Asia/Shanghai";
        //String url = "jdbc:mysql://localhost:3306/mydb?serverTimezone=Asia/Shanghai";
        //MySQL配置时的用户名
        String user = "root";
        //MySQL配置时的密码
        //String password = "314159";
        String password = "fionasit61";
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
            //preparedStatement3 = con.prepareStatement("drop table if exists user_behaviors;");
            preparedStatement1 = con.prepareStatement("drop table if exists users;");
            preparedStatement2 = con.prepareStatement("drop table if exists articles;");
            //preparedStatement3.executeUpdate();
            preparedStatement1.executeUpdate();
            preparedStatement2.executeUpdate();

            //创建新表
            createStatement1 = con.createStatement();
            createStatement2 = con.createStatement();
            createStatement3 = con.createStatement();
            createStatement1.executeUpdate("create table if not exists users (" +
                    "userid varchar(100) not null primary key," +
                    "gender integer(1) not null," +
                    "status integer(1) not null," +
                    "level integer(1) not null," +
                    "degree integer(2)," +
                    "school varchar(255)," +
                    "major varchar(255)," +
                    "skill text," +
                    "interest text);");
            createStatement2.executeUpdate("create table if not exists articles (" +
                    "articleid varchar(100) not null primary key," +
                    "type integer(10) not null," +
                    "istop integer(2) not null," +
                    "status integer(2) not null," +
                    "domain varchar(255) not null);");
            /*createStatement3.executeUpdate("create table if not exists user_behaviors (" +
                    "userid varchar(100) not null," +
                    "behavior integer not null," +
                    "articleid varchar(100) not null," +
                    "behaviortime datetime(1) not null," +
                    "foreign key(userid) references users(userid)," +
                    "foreign key(articleid) references articles(articleid));");*/

            //插入数据
            preparedStatement1 = con.prepareStatement("insert users values(?,?,?,?,?,?,?,?,?);");
            preparedStatement2 = con.prepareStatement("insert articles values(?,?,?,?,?);");
            length = userBasicList.size();
            for (int i = 0; i < length; i++) {
                String[] userBasicArray = userBasicList.get(i).split("\u0001");
                String[] userEduArray = userEduList.get(i).split("\u0001");
                String[] userSkillArray = userSkillList.get(i).split("\u0001");
                String[] userInterestArray = userInterestList.get(i).split("\u0001");
                preparedStatement1.setString(1, userBasicArray[0]);
                preparedStatement1.setInt(2, Integer.parseInt(userBasicArray[1]));
                preparedStatement1.setInt(3, Integer.parseInt(userBasicArray[2]));
                /* level字段按整型存储
                3: 专家
                2: 精英
                1: 普通
                0: 小白
                */
                switch (userBasicArray[3]){
                    case "专家":
                        preparedStatement1.setInt(4, 3);
                        break;
                    case "精英":
                        preparedStatement1.setInt(4, 2);
                        break;
                    case "普通":
                        preparedStatement1.setInt(4, 1);
                        break;
                    case "小白":
                        preparedStatement1.setInt(4, 0);
                        break;
                }
                if(userEduArray.length>3) {
                    if(!userEduArray[1].trim().isEmpty())preparedStatement1.setInt(5, Integer.parseInt(userEduArray[1]));
                    if(!userEduArray[2].trim().isEmpty())preparedStatement1.setString(6, userEduArray[2]);
                    if(!userEduArray[3].trim().isEmpty())preparedStatement1.setString(7, userEduArray[3]);
                }else if(userEduArray.length>2) {
                    if(!userEduArray[1].trim().isEmpty())preparedStatement1.setInt(5, Integer.parseInt(userEduArray[1]));
                    if(!userEduArray[2].trim().isEmpty())preparedStatement1.setString(6, userEduArray[2]);
                    preparedStatement1.setString(7, null);
                }else if(userEduArray.length>1) {
                    if(!userEduArray[1].trim().isEmpty())preparedStatement1.setInt(5, Integer.parseInt(userEduArray[1]));
                    preparedStatement1.setString(6, null);
                    preparedStatement1.setString(7, null);
                }else {
                    preparedStatement1.setObject(5, null);
                    preparedStatement1.setString(6, null);
                    preparedStatement1.setString(7, null);
                }
                System.out.println("Skill of user '"+userBasicArray[0]+"': "+userSkillArray[1]);
                preparedStatement1.setString(8, userSkillArray[1]);
                preparedStatement1.setString(9, userInterestArray[1]);
                preparedStatement1.executeUpdate();
            }
            length = articleInfoList.size();
            for (int i = 0; i < length; i++) {
                String[] strArray = articleInfoList.get(i).split("\u0001");
                preparedStatement2.setString(1, strArray[0]);
                preparedStatement2.setInt(2, Integer.parseInt(strArray[1]));
                preparedStatement2.setInt(3, Integer.parseInt(strArray[2]));
                preparedStatement2.setInt(4, Integer.parseInt(strArray[3]));
                System.out.println("Domain of article '"+strArray[0]+"': "+strArray[4]);
                preparedStatement2.setString(5, strArray[4]);
                preparedStatement2.executeUpdate();
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

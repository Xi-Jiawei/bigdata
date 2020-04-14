package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class HBaseExperiment {
    public static void main(String[] args) throws Throwable {
        try {
            /*连接HBase*/
            Configuration configuration;
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            //configuration.set("hbase.zookeeper.quorum", "172.31.42.237,172.31.43.12,172.31.43.21");
            configuration.set("hbase.zookeeper.quorum", "192.168.61.130,192.168.61.131,192.168.61.132");
            //configuration.set("hbase.zookeeper.quorum", "47.98.176.164,47.98.47.81,116.62.119.79");
            //configuration.set("hbase.master", "172.31.42.237:60010");
            configuration.set("hbase.master", "192.168.61.130:60010");
            //configuration.set("hbase.master", "47.98.176.164:60010");
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("userBehaviors");
            /* 删除旧表
             * 如果存在要创建的表，那么先删除，再创建*/
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            /*创建新表*/
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

            /*读取数据，并插入到HBase*/
            /*获取Table对象*/
            Table table = connection.getTable(tableName);
            // BufferedMutator替代Table进行插入，关闭hbase自动flush，手动flush。这是关键设置，可以有效解决写入大量数据时内存爆满的问题
            // 旧版API：“HTable table=new HTable(conf,TableName.valueOf(tableName));table.setAutoFlush(false);table.setWriteBufferSize(1024 * 1024);table.put(put);table.flushCommits();”
            BufferedMutatorParams bufferedMutatorParams=new BufferedMutatorParams(tableName).writeBufferSize(16 * 1024 * 1024);//设置16M缓冲区，缓冲区满后数据会自动flush
            BufferedMutator bufferedMutator=connection.getBufferedMutator(bufferedMutatorParams);
            Put put;
            PreparedStatement preparedStatement;
            ResultSet resultSet;
            int index=0;
            //读取用户行为数据
            List<UserBehavior> userBehaviorList=new ArrayList<UserBehavior>();
            String line;
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("D:\\Files\\Documents\\Study\\Curriculums\\研究生\\大数据\\Datasets_3\\数据仓库课程实验三数据\\userBehavior\\userBehavior"))));
            //BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/usr/local/src/Datasets_3/userBehavior/userBehavior"))));
            /*while ((line = reader.readLine()) != null) {
                index++;
                System.out.println("index: "+index);
                System.out.println(line);

                String[] strArray = line.split("\u0001");//以"\u0001"分割字符串

                put = new Put(Bytes.toBytes(index));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes(strArray[0]));//参数：1.列族名  2.列名  3.值
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(Integer.parseInt(strArray[1])));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("articleid"), Bytes.toBytes(strArray[2]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), Bytes.toBytes(strArray[3]));

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

                table.put(put);
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
            System.gc();
            for(UserBehavior userBehavior:userBehaviorList) {
                index++;
                System.out.println("index: "+index);
                System.out.println(userBehavior.getUserid()+"\u0001"+userBehavior.getBehavior()+"\u0001"+userBehavior.getArticleid()+"\u0001"+userBehavior.getBehaviortime());

                put = new Put(Bytes.toBytes(index));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userid"), Bytes.toBytes(userBehavior.getUserid()));//参数：1.列族名  2.列名  3.值
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(Integer.parseInt(userBehavior.getBehavior())));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("articleid"), Bytes.toBytes(userBehavior.getArticleid()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behaviortime"), Bytes.toBytes(userBehavior.getBehaviortime()));

                preparedStatement = con.prepareStatement("select level,degree from users where userid='"+userBehavior.getUserid()+"';");
                resultSet=preparedStatement.executeQuery();
                if(resultSet.next()) {
                    int level = resultSet.getInt(1);//注意，MySQL API均以1开始，而不是以0开始
                    Object degree = resultSet.getObject(2);//int degree = resultSet.getInt(2);
                    System.out.println("Degree of user '"+userBehavior.getUserid()+"': "+degree);

                    //插入数据
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(level));
                    if(degree!=null)put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("degree"), Bytes.toBytes((int)degree));
                }
                resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                preparedStatement = con.prepareStatement("select domain from articles where articleid='"+userBehavior.getArticleid()+"';");
                resultSet=preparedStatement.executeQuery();
                if(resultSet.next()) {
                    String domain = resultSet.getString(1);
                    //System.out.println("Domain of article '"+userBehavior.getArticleid()+"': "+domain);

                    //插入数据
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("domain"), Bytes.toBytes(domain));
                }
                resultSet.close();//这是关键设置，释放mysql占用内存，否则内存会一直增加直到爆满

                //table.put(put);
                bufferedMutator.mutate(put);
                if(index%10000==0)bufferedMutator.flush();
                if(index%100000==0)System.gc();
            }

            bufferedMutator.flush();
            System.out.println("Operation finished!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

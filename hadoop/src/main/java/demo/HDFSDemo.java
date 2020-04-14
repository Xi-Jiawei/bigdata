package demo;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class HDFSDemo {
    public static void main(String[] args) {
        Configuration configuration=new Configuration();
        //configuration.set("fs.defaultFS","hdfs://192.168.61.130:9000");//configuration.set("fs.defaultFS","hdfs://cluster1:9000");
        configuration.set("fs.defaultFS","hdfs://47.98.176.164:9000");//configuration.set("fs.defaultFS","hdfs://cluster1:9000");
        System.setProperty("HADOOP_USER_NAME","hadoop");
        FileSystem fileSystem=null;

        try {
            /*连接hdfs，获取FileSystem对象*/
            fileSystem=FileSystem.get(configuration);

            /*新建文件夹*/
            //fileSystem.mkdirs(new Path("/test/experiment"));

            /*上传下载*/
            /*fileSystem.delete(new Path("/test/experiment/userBehaviors"),false);
            fileSystem.copyFromLocalFile(new Path("D:\\Users\\veev\\BigData\\userBehaviors"), new Path("/test/experiment/userBehaviors"));
            //fileSystem.copyToLocalFile(new Path("/test/experiment/userBehaviors"),new Path("D:\\Users\\veev\\BigData\\download"));*/
            //fileSystem.copyFromLocalFile(new Path("D:\\Users\\veev\\BigData\\userBehavior"), new Path("/test/experiment/userBehavior"));

            /*删写读*/
            /*Path path=new Path("/test/out");
            //删
            fileSystem.delete(path,false);//第二个参数表示是否递归：如果true，相当于命令“hdfs dfs -rm -r”，删除文件夹；如果false，相当于命令“hdfs dfs -rm”，删除文件
            //写
            FSDataOutputStream out = fileSystem.create(path);
            //IOUtils.copyBytes(new ByteArrayInputStream("test".getBytes()),out, 4096,true);
            for(int i=0;i<5;i++)
                IOUtils.copyBytes(new ByteArrayInputStream((Integer.toString(i)+"\n").getBytes()), out,4096,false);
            out.close();//关闭流，才能真正写入文件。关闭fileSystem对象“fileSystem.close();”也会触发此行为
            //读
            //FSDataInputStream in = fileSystem.open(path);
            FSDataInputStream in = fileSystem.open(new Path("/test/userBehaviors"));
            IOUtils.copyBytes(in,System.out, 4096,true);//输出文件内容到控制台System.out*/

            /*byte[] bytes1=Bytes.toBytes(1);
            byte[] bytes2=Integer.toString(1).getBytes();
            byte[] bytes3=Integer.toString(1).getBytes("unicode");
            byte[] bytes4=Integer.toString(1).getBytes("utf-8");
            byte[] bytes5=Integer.toString(1).getBytes("utf-16");
            byte[] bytes6=Integer.toString(1).getBytes("utf-32");
            byte[] bytes7=Integer.toString(1).getBytes("gbk");
            byte[] bytes8=Integer.toString(1).getBytes("iso8859-1");
            System.out.println(bytes1.length+""+bytes2.length+""+bytes3.length+""+bytes4.length+""+bytes5.length+""+bytes6.length+""+bytes7.length+""+bytes8.length);*/

            /*FileOutputStream的写与FileInputStreamr的读*/
            /*String path="D:\\Users\\veev\\BigData\\userBehaviors";
            //写
            FileOutputStream out=new FileOutputStream(path);
            for(int i=0;i<5;i++)
                out.write((Integer.toString(i)+"\n").getBytes("utf-8"));
            out.close();//关闭流，才能真正写入文件
            //读
            FileInputStream in=new FileInputStream(path);
            List<Byte> byteList=new ArrayList<Byte>();
            int readByte=in.read();//每次读取1个字节数据，并将其由二进制转成十进制的整数返回。如果因为已经到达文件末尾而没有更多的数据，则返回-1
            while (readByte!=-1) {
                byteList.add((byte)readByte);//因为readByte是由8位二进制数转成的十进制整数，所以在此可以直接强制类型转换
                readByte = in.read();
            }
            in.close();
            byte[] bytes= Bytes.toArray(byteList);//com.google.common.primitives.Bytes
            System.out.println(new String(bytes,"utf-8"));*/

            /*与BufferedReader的读*/
            /*String path="D:\\Users\\veev\\BigData\\userBehaviors";
            //写
            FileWriter writer = new FileWriter(path);
            for(int i=0;i<5;i++) {
                writer.write(Integer.toString(i));
                writer.write("\n");
            }
            writer.close();
            //读
            FileReader reader=new FileReader(path);
            List<Byte> byteList=new ArrayList<Byte>();
            int readByte = reader.read();//同FileInputStream，每次读取1个字节数据，并将其由二进制转成十进制的整数返回。如果因为已经到达文件末尾而没有更多的数据，则返回-1
            while(readByte!= -1) {
                byteList.add((byte)readByte);//因为readByte是由8位二进制数转成的十进制整数，所以在此可以直接强制类型转换
                readByte = reader.read();
            }
            reader.close();
            byte[] bytes= Bytes.toArray(byteList);//com.google.common.primitives.Bytes
            System.out.println(new String(bytes));*/

            /*BufferedWriter的读与BufferedReader的读*/
            /*String path="D:\\Users\\veev\\BigData\\userBehaviors";
            //写
            BufferedWriter bufferedWriter=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(path))));
            //FileWriter writer=new FileWriter(path);
            //BufferedWriter bufferedWriter=new BufferedWriter(writer);
            for(int i=0;i<5;i++) {
                bufferedWriter.write(i);
                //bufferedWriter.write(Integer.toString(i));
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
            //读
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
            //FileReader reader=new FileReader(path);
            //BufferedReader bufferedReader = new BufferedReader(reader);
            String line=bufferedReader.readLine();
            while(line!= null) {
                System.out.println((int)line.getBytes()[0]);//对应写入语句“bufferedWriter.write(i);”。因为是直接将数字写入文件，所以读取时直接将字节转数字
                //System.out.println(line);//对应写入语句“bufferedWriter.write(Integer.toString(i));”
                line=bufferedReader.readLine();
            }
            bufferedReader.close();*/

            fileSystem.close();

            System.out.println("Operation finished!");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            /*try {
                fileSystem.close();
            }catch (Exception e){
                e.printStackTrace();
            }*/
        }
    }
}

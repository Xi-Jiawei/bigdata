package demo;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.utils.Utils;

/* * Storm的grouping策略就是在Spout与Bolt、Bolt与Bolt之间传递Tuple的方式。总共有七种方式：
 * 1）shuffleGrouping（随机分组）
 * 2）fieldsGrouping（按照字段分组，在这里即是同一个单词只能发送给一个Bolt）
 * 3）allGrouping（广播发送，即每一个Tuple，每一个Bolt都会收到）
 * 4）globalGrouping（全局分组，将Tuple分配到task id值最低的task里面）
 * 5）noneGrouping（随机分派）
 * 6）directGrouping（直接分组，指定Tuple与Bolt的对应发送关系）
 * 7）Local or shuffle Grouping
 * 8）customGrouping （自定义的Grouping）
 * * */
public class StormDemo {
    public static void main(String[] args) throws Exception {
        /*构造拓扑
        * 在这里有三层：
        * 1、Spout提供数据源
        * 2、SplitBolt分词
        * 3、CountBolt统计数量*/
        TopologyBuilder builder = new TopologyBuilder();//定义一个拓扑
        builder.setSpout("spout", new Spout());//设置1个Executor（线程），默认1个，builder.setSpout("spout", new demo.Spout(), 1);
        builder.setBolt("splitBolt", new SplitBolt()).setNumTasks(1).shuffleGrouping("spout");//设置1个Executor，2个Task。随机分组，无论Spout发出任何数据，即使发出同样字段的数据时，处理该数据的task是随机的
        builder.setBolt("countBolt", new CountBolt()).setNumTasks(1).fieldsGrouping("splitBolt",new Fields("word"));//设置1个Executor，1个Task。按照字段分组，即同样字段的数据只能发送给一个Task实例处理，这样保证结果正确

        /*配置*/
        Config config = new Config();
        //config.setNumWorkers(2);//设置topology在集群中运行所需要的的工作进程数量，因为集群只有三台安装了Storm，所以这个数量不能大于3。默认一个topology使用一个工作进程worker
        //config.setNumAckers(0);//设置__acker数量为0个，这样就不会有其executor线程
        //config.put("test", "test");//自定义的配置，没有实际意义
        //config.setMessageTimeoutSecs(60);
        //config.put(Config.TOPOLOGY_WORKER_CHILDOPTS,"-Xmx2048m -Xms2048m -Xmn384m -XX:PermSize=128m -XX:+UseConcMarkSweepGC");

        /*提交运行*/
        if (args != null && args.length > 0) {
            //提交到集群运行
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            //本地模式运行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("StormClient", config, builder.createTopology());
            Utils.sleep(1000000);
            cluster.killTopology("StormClient");
            cluster.shutdown();
        }
    }
}

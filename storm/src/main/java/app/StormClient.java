package app;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

public class StormClient {
    public static class SplitBolt extends BaseRichBolt {
        private OutputCollector collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            //String sentence = tuple.getStringByField("sentence");//根据Spout中declareOutputFields()定义的数据格式，接收来自Spout的数据
            Object tupleValues = tuple.getValues();//当Spout没有定义数据格式，则通过getValues()接收来自Spout的数据
            System.out.println("consumer receive: "+tuple.getValues());
            String sentence = (String) tuple.getValue(4);//来自KafkaSpout的消息以"[mykafka, 0, 52, null, hello world]"形式封装
            String[] words = sentence.split(" ");//分词

            for(String word:words){
                this.collector.emit(new Values(word,1));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));
        }
    }

    public static class CountBolt extends BaseRichBolt {
        private OutputCollector collector;
        private Map<String,Integer> result = new HashMap<String,Integer>();//定义一个Map集合保存最后的统计结果

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            //接收来自SplitBolt的数据
            String word = tuple.getStringByField("word");
            int count = tuple.getIntegerByField("count");

            //判断一个result是否存在该单词
            if(this.result.containsKey(word)){
                //包含该单词
                int total = this.result.get(word);
                this.result.put(word, total+count);
            }else{
                //不存在该单词
                this.result.put(word, count);
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void cleanup() {
            for (Map.Entry<String, Integer> entry : this.result.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        /*构造拓扑
         * 在这里有三层：
         * 1、KafkaSpout提供数据源
         * 2、SplitBolt分词
         * 3、CountBolt统计数量*/
        TopologyBuilder builder = new TopologyBuilder();//定义一个拓扑
        //builder.setSpout("spout", new KafkaSpout<>(KafkaSpoutConfig.builder("192.168.61.130:9092","mykafka").setProp("group.id", "test-consumer-group").build()));//设置1个Executor（线程），默认1个，builder.setSpout("spout", new demo.Spout(), 1);
        builder.setSpout("spout", new KafkaSpout<>(KafkaSpoutConfig.builder("47.98.176.164:9092","mykafka").setProp("group.id", "test-consumer-group").build()));//设置1个Executor（线程），默认1个，builder.setSpout("spout", new demo.Spout(), 1);
        builder.setBolt("splitBolt", new SplitBolt()).setNumTasks(1).shuffleGrouping("spout");//设置1个Executor，2个Task。随机分组，无论Spout发出任何数据，即使发出同样字段的数据时，处理该数据的task是随机的
        builder.setBolt("countBolt", new CountBolt()).setNumTasks(1).fieldsGrouping("splitBolt",new Fields("word"));//设置1个Executor，1个Task。按照字段分组，即同样字段的数据只能发送给一个Task实例处理，这样保证结果正确

        /*配置*/
        Config config = new Config();

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

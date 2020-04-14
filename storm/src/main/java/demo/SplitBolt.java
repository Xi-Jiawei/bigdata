package demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt extends BaseRichBolt {
    private OutputCollector collector;

    /* * Bolt的prepare()方法如同Spout的open()方法，在该Bolt组件初始化时调用
     * 参数和Spout一样，区别在于发射器换成OutputCollector对象
     * * 在这里，我们也不执行额外的操作，只作组件初始化。*/
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    /* * execute()方法是Bolt的核心，也就是Bolt的主要执行函数。
     * * Storm调用这个方法，进行数据的接收和发送：
     * 1、通过tuple.getStringByField()方法接收来自Spout的数据
     * 2、通过collector.emit()方法向下一个Bolt输出数据
     * */
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");//根据Spout中declareOutputFields()定义的数据格式，接收来自Spout的数据
        String[] words = sentence.split(" ");//分词

        //System.out.println("recieved message: "+sentence);

        //输出数据
        for(String word:words){
            this.collector.emit(new Values(word,1));
        }
    }

    /* * 同Spout中的declareOutputFields()方法一样，用于声明该组件输出数据的格式
     * */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }
}

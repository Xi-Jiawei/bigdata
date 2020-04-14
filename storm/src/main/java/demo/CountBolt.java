package demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String,Integer> result = new HashMap<String,Integer>();//定义一个Map集合保存最后的统计结果

    /* * Bolt的prepare()方法如同Spout的open()方法，在该Bolt组件初始化时调用
     * 参数和Spout一样，区别在于发射器换成OutputCollector对象
     * * 在这里，我们也不执行额外的操作，只作组件初始化。*/
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    /* * execute()方法是Bolt的核心，也就是Bolt的主要执行函数。
     * * Storm调用这个方法，进行数据的接收和发送：
     * 1、通过tuple.getStringByField()方法接收来自上一个Bolt的数据
     * 2、通过collector.emit()方法向下一个Bolt输出数据。因为这里是末端bolt，不需要发射数据流，所以就没有调用collector.emit()方法
     * */
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

        //System.out.println("count of ["+word+"]: "+this.result.get(word));
    }

    /* * 同Spout中的declareOutputFields()方法一样，用于声明该组件输出数据的格式
     * 因为这里是末端bolt，不需要发射数据流，这里无需定义
     * */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    /* * cleanup()是IBolt接口中定义，在结束该Bolt组件时调用，用于释放Bolt组件占用资源。
     * * 在这个例子中，在统计好结果后，在这里打印出来。*/
    @Override
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : this.result.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}

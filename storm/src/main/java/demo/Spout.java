package demo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class Spout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;

    //在Spout中准备好要处理的数据源
    /*private String[] messageArray={
            "LiuYan is my freind",
            "her birthday is on December 2nd",
            "her student number is 18140070"
    };*/
    private String[] messageArray={
            "my name is xijiawei",
            "and i am 27 years old",
            "i am from a small county of Ji'an, Jiangxi in China"
    };
    private int i=0;

    /* * open()方法中是ISpout接口中定义，在Spout组件初始化时被调用。
     * open()接受三个参数：
     * 1、一个包含Storm配置的Map
     * 2、一个TopologyContext对象，提供了topology中组件的信息
     * 3、SpoutOutputCollector对象提供发射tuple的方法。
     * * 在这个例子中，我们不需要执行额外的操作，只初始化发射器。*/
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /* * nextTuple()方法是任何Spout实现的核心，也就是Spout的主要执行函数。
     * Storm调用这个方法，向Bolt输出数据，通过collector.emit方法发射
     * */
    public void nextTuple() {
//        for(String message:messageArray) {
//            this.collector.emit(new Values(message));
//            System.out.println("sending message: "+message);
//            Utils.sleep(3000);
//        }

        if(i<messageArray.length){
            this.collector.emit(new Values(messageArray[i]));
            //System.out.println("sending message: "+messageArray[i]);
            i++;
            Utils.sleep(3000);
        }
    }

    /* * declareOutputFields()是在IComponent接口中定义的，所有Storm的组件（spout和bolt）都必须实现这个接口
     * 这个方法用于声明Spout输出数据的格式，也就是告诉接收的Bolt组件有哪些数据流
     * */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}

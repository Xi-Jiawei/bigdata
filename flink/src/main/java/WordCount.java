import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String hostname = params.get("hostname");
        int port = params.getInt("port");
        /*//String hostname = "192.168.61.131";
        String hostname = "47.98.176.164";
        int port = 9999;*/

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.socketTextStream(hostname, port);

        DataStream<Tuple2<String, Integer>> windowCounts = stream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String str, Collector<Tuple2<String, Integer>> collector) {
                        for (String word : str.split("\\s")) {
                            collector.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })
                .keyBy(0)//以key分组统计
                .timeWindow(Time.seconds(5), Time.seconds(1))//定义一个5s的滑动时间窗口，每1s滑动一次
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
                        return new Tuple2<String, Integer>(tuple1.f0, tuple1.f1+tuple2.f1);
                    }
                });
        windowCounts.print();

        env.execute();
    }
}

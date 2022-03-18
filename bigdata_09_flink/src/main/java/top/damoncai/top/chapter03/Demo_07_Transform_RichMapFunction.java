package top.damoncai.top.chapter03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.damoncai.top.bean.Event;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/18 10:42
 */
public class Demo_07_Transform_RichMapFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        SingleOutputStreamOperator<Integer> myMap = stream.map(new MyMap()).setParallelism(2);

        myMap.print();

        env.execute();
    }
}

class MyMap extends RichMapFunction<Event,Integer> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open:" + getRuntimeContext().getIndexOfThisSubtask() + "号启动了");
    }

    @Override
    public Integer map(Event event) throws Exception {
        return event.user.length();
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("close:" + getRuntimeContext().getIndexOfThisSubtask() + "号关闭了");
    }
}

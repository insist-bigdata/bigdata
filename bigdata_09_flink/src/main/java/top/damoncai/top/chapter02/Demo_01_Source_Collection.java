package top.damoncai.top.chapter02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import top.damoncai.top.bean.Event;

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/18 10:42
 */
public class Demo_01_Source_Collection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Event> events = Arrays.asList(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        DataStreamSource<Event> stream = env.fromCollection(events);

        //我们也可以不构建集合，直接将元素列举出来，调用 fromElements 方法进行读取数据
        DataStreamSource<Event> stream2 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.print("1");
        stream2.print("2");

        env.execute();
    }
}

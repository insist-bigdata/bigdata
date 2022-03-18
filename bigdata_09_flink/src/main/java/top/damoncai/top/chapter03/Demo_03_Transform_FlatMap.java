package top.damoncai.top.chapter03;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import top.damoncai.top.bean.Event;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/18 10:42
 */
public class Demo_03_Transform_FlatMap {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        SingleOutputStreamOperator<String> flatMap = stream.flatMap(
                (Event event, Collector<String> out) -> {
                    out.collect(event.user);
                    out.collect(event.url);
                    out.collect(event.timestamp + "");
                }
        ).returns(new TypeHint<String>() {});

        flatMap.print();

        env.execute();
    }
}

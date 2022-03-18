package top.damoncai.top.chapter03;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/18 10:42
 */
public class Demo_06_Aggregation_Reduce {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce =
                stream
                        .keyBy(tuple -> tuple.f0)
                        .reduce((e1, e2) -> Tuple2.of(e1.f0, e1.f1 + e2.f1));

        reduce.print();

        env.execute();
    }
}

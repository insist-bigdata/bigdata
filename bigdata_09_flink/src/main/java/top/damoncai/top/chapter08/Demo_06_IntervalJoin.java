package top.damoncai.top.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import top.damoncai.top.bean.Event;

import java.sql.Timestamp;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 14:09
 */
public class Demo_06_IntervalJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream =
                env.fromElements(
                        Tuple3.of("Mary", "order-1", 5000L),
                        Tuple3.of("Alice", "order-2", 5000L),
                        Tuple3.of("Bob", "order-3", 20000L),
                        Tuple3.of("Alice", "order-4", 20000L),
                        Tuple3.of("Cary", "order-5", 51000L)
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                       @Override
                                       public long extractTimestamp(Tuple3<String, String, Long>
                                                                            element, long recordTimestamp) {
                                           return element.f2;
                                       }
                                   })
                );

        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./prod?id=1", 23000L),
                new Event("Bob", "./prod?id=3", 33000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>()
                        {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp)
                            {
                                return element.timestamp;
                            }
                        })
        );
        orderStream.keyBy(data -> data.f0)
                        .intervalJoin(clickStream.keyBy(event -> event.user))
                                .between(Time.seconds(-5), Time.seconds(10))
                                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>() {
                                    @Override
                                    public void processElement(Tuple3<String, String, Long> right, Event left, ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>.Context context, Collector<String> out) throws Exception {
                                        out.collect(right + " => " + left);
                                    }
                                }).print();


        env.execute();
    }
}

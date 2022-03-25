package top.damoncai.top.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import top.damoncai.top.bean.Event;
import top.damoncai.top.chapter02.Demo_03_Source_Customer;

import java.time.Duration;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 14:09
 */
public class Demo_04_State_TwoStreamFullJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env
                .fromElements(
                        Tuple3.of("a", "stream-1", 1000L),
                        Tuple3.of("b", "stream-1", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String,
                                        Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                               @Override
                                               public long extractTimestamp(Tuple3<String,
                                                       String, Long> t, long l) {
                                                   return t.f2;
                                               }
                                           })
                );
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env
                .fromElements(
                        Tuple3.of("a", "stream-2", 3000L),
                        Tuple3.of("b", "stream-2", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String,
                                        Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                       @Override
                                       public long extractTimestamp(Tuple3<String,
                                               String, Long> t, long l) {
                                           return t.f2;
                                       }
                                   })
                );

        stream1
                .connect(stream2)
                .keyBy(e1 -> e1.f0, e2 -> e2.f0)
                        .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {

                            ListState<Tuple3<String, String, Long>> first;
                            ListState<Tuple3<String, String, Long>> second;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                first = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("first", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));
                                second = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("second", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));
                            }

                            @Override
                            public void processElement1(Tuple3<String, String, Long> firstItem, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                                for (Tuple3<String, String, Long> second : second.get()) {
                                    System.out.println(firstItem + " -> " + second);
                                }
                                first.add(firstItem);
                            }

                            @Override
                            public void processElement2(Tuple3<String, String, Long> secondItem, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                                for (Tuple3<String, String, Long> first : first.get()) {
                                    System.out.println(secondItem + " -> " + first);
                                }
                                second.add(secondItem);
                            }
                        }).print();

        env.execute();
    }
}

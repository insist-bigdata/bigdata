package top.damoncai.top.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.damoncai.top.bean.Event;
import top.damoncai.top.chapter02.Demo_03_Source_Customer;

import java.sql.Timestamp;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 14:09
 */
public class Demo_06_State_AverageTimestamp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new Demo_03_Source_Customer.ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                               @Override
                                               public long extractTimestamp(Event event, long l) {
                                                   return event.timestamp;
                                               }
                                           })
                );

        stream.keyBy(data -> data.user)
                        .flatMap(new RichFlatMapFunction<Event, String>() {


                            AggregatingState<Event,Tuple2<Long, Long>> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                state = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long,Long>, Tuple2<Long, Long>>(
                                        "state",
                                        new AggregateFunction<Event, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                                            @Override
                                            public Tuple2<Long, Long> createAccumulator() {
                                                return Tuple2.of(0L, 0L);
                                            }

                                            @Override
                                            public Tuple2<Long, Long> add(Event event, Tuple2<Long, Long> tuple) {
                                                return Tuple2.of(tuple.f0 + event.timestamp , tuple.f1 + 1);
                                            }

                                            @Override
                                            public Tuple2<Long, Long> getResult(Tuple2<Long, Long> tuple) {
                                                return tuple;
                                            }

                                            @Override
                                            public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                                                return null;
                                            }
                                        },
                                        Types.TUPLE(Types.LONG, Types.LONG)));
                            }

                            @Override
                            public void flatMap(Event event, Collector<String> collector) throws Exception {
                                System.out.println(event);
                                state.add(event);
                                Tuple2<Long, Long> tuple = state.get();
                                if(tuple.f1 == 5) {
                                    collector.collect(event.user + " -> " + tuple.f0 / tuple.f1);
                                    state.clear();
                                }
                            }
                        })
                        .print();
        env.execute();
    }
}

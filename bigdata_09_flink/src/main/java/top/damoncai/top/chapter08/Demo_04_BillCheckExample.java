package top.damoncai.top.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 14:09
 */
public class Demo_04_BillCheckExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long l) {
                                return element.f2;
                            }
                        })
        );

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> element, long l) {
                                return element.f3;
                            }
                        })
        );

        // ?????????????????????????????????????????????????????????????????????
        appStream.connect(thirdpartStream)
                .keyBy(e1 -> e1.f0, e2 -> e2.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
                    private ValueState<Tuple3<String, String, Long>> appState;
                    private ValueState<Tuple4<String, String, String, Long>> thirdPartState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        appState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING,Types.LONG)));
                        thirdPartState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String,String, Long>>("thirdParty-event", Types.TUPLE(Types.STRING, Types.STRING,Types.STRING,Types.LONG)));

                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> out) throws Exception {
                        if(thirdPartState.value() == null) { // ???????????????????????????
                            // ????????????
                            appState.update(value);
                            // ???????????????
                            context.timerService().registerEventTimeTimer(value.f2 + 5000L);
                        }else{// ???????????????????????????
                            out.collect(" ??? ??? ??? ??? ??? " + value + " " + thirdPartState.value());
                            // ????????????
                            thirdPartState.clear();
                        }
                    }

                    @Override
                    public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> out) throws Exception {

                        if(appState.value() == null) { // ???????????????????????????
                            // ????????????
                            thirdPartState.update(value);
                            // ???????????????
                            context.timerService().registerEventTimeTimer(value.f3 + 5000L);
                        }else{// ???????????????????????????
                            out.collect(" ??? ??? ??? ??? ??? " + value + " " + appState.value());
                            // ????????????
                            appState.clear();
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

                        // ????????????????????????????????????????????????????????????????????????????????????????????????
                        if (appState.value() != null) {
                            out.collect("???????????????" + appState.value() + " " + "??????????????? ??????????????????");
                        }
                        if (thirdPartState.value() != null) {
                            out.collect("???????????????" + thirdPartState.value() + " " + "app ????????????");
                        }
                        appState.clear();
                        thirdPartState.clear();

                    }
                }).print();

        env.execute();
    }
}

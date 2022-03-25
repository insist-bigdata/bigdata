package top.damoncai.top.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.damoncai.top.bean.Event;
import top.damoncai.top.chapter02.Demo_03_Source_Customer;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 14:09
 */
public class Demo_03_State_PeriodicPvExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new Demo_03_Source_Customer.ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                );

        stream.keyBy(data -> data.user)
            .process(new KeyedProcessFunction<String, Event, String>() {
                private ValueState<Long> count;
                private ValueState<Long> ts;

                @Override
                public void open(Configuration parameters) throws Exception {
                    count = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
                    ts = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts",Long.class));
                }

                @Override
                public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> out) throws Exception {
                    System.out.println("生成数据：" + event);
                    // 更新 count 值
                    Long value = count.value();
                    if (value == null){
                        count.update(1L);
                    } else {
                        count.update(value + 1);
                    }

                    Long ts = this.ts.value();
                    if(ts == null) {
                        // 注册定时器
                        context.timerService().registerEventTimeTimer(event.timestamp + 10 * 1000L);
                        this.ts.update(event.timestamp);
                    }

                }

                @Override
                public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

                    System.out.println(ctx.getCurrentKey() + "->" + count.value());
                    System.out.println("ts: " + ts.value());
                    ts.update(null);
                }

            }).print();

        env.execute();
    }
}

package top.damoncai.top.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
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
public class Demo_05_State_FakeWindow {

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

        stream.keyBy(data -> data.url)
                        .process(new MyWindowKeyprocessFunction(10000L))
                        .print();


        env.execute();
    }

    public static class MyWindowKeyprocessFunction extends KeyedProcessFunction<String,Event,String> {

        private Long duration;
        private MapState<Long,Long> windowState;

        public MyWindowKeyprocessFunction (Long duration) {
            this.duration = duration;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("windowState",Long.class,Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            System.out.println(event);
            // 获取窗口起始时间
            Long ts = event.timestamp;
            long windowStart = ts / duration * duration;
            long windowEnd = windowStart + duration;

            // 判断并创建定时器
            Long count = windowState.get(windowStart);
            if(count == null) {
                context.timerService().registerEventTimeTimer(windowEnd - 1);
                windowState.put(windowStart,1L);
            }else {
                windowState.put(windowStart,count + 1);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            long windowEnd = timestamp + 1;
            long windowStart = windowEnd - duration;
            System.out.println("窗口时间范围为：" + new Timestamp(windowStart) + " -> " + new Timestamp(windowEnd));
            System.out.println(ctx.getCurrentKey() + " -> " + windowState.get(windowStart));
            // 清除
            windowState.remove(windowStart);
            System.out.println("============================");
        }

    }
}

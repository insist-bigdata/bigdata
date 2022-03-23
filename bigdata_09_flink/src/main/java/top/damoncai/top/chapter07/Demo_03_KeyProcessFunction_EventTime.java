package top.damoncai.top.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
public class Demo_03_KeyProcessFunction_EventTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Event>() {
                                           @Override
                                           public long extractTimestamp(Event element, long recordTimestamp) {
                                               return element.timestamp;
                                           }
                                       }));
        // 要用定时器，必须基于 KeyedStream
        stream.keyBy(data -> true)
                        .process(new KeyedProcessFunction<Boolean, Event, String>() {

                            @Override
                            public void processElement(Event event, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
                                out.collect("数据到达，时间戳为：" + ctx.timestamp());
                                out.collect(" 数 据 到 达 ， 水 位 线 为 ： " + ctx.timerService().currentWatermark() + "\n -------分割线-------");
                                // 注册一个 10 秒后的定时器
                                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10 * 1000L);
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                            }
                        }).print();


        env.execute();
    }

    // 自定义测试数据源
    public static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));
            // 为了更加明显，中间停顿 5 秒钟
            Thread.sleep(5000L);
            // 发出 10 秒后的数据
            ctx.collect(new Event("Mary", "./home", 11000L));
            Thread.sleep(5000L);
            // 发出 10 秒+1ms 后的数据
            ctx.collect(new Event("Alice", "./cart", 11001L));
            Thread.sleep(5000L);
        }
        @Override
        public void cancel() { }
    }
}

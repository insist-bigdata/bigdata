package top.damoncai.top.chapter07;

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
public class Demo_02_KeyProcessFunction_ProcessTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new Demo_03_Source_Customer.ClickSource());

        // 要用定时器，必须基于 KeyedStream
        stream.keyBy(data -> true)
                        .process(new KeyedProcessFunction<Boolean, Event, String>() {

                            @Override
                            public void processElement(Event event, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
                                long currTs = ctx.timerService().currentProcessingTime();
                                out.collect("数据到达，到达时间：" + new Timestamp(currTs));
                                ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                            }
                        }).print();


        env.execute();
    }
}

package top.damoncai.top.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
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
public class Demo_01_SplitStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new Demo_03_Source_Customer.ClickSource());

        OutputTag<String> mary = new OutputTag<String>("mary"){};
        OutputTag<String> bolb = new OutputTag<String>("bob"){};

        SingleOutputStreamOperator<String> processStream = stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                String user = event.user;
                if (user.equals("Mary")) {
                    context.output(mary, event.toString());
                } else if (user.equals("Bob")) {
                    context.output(bolb, event.toString());
                } else {
                    collector.collect(event.toString());
                }
            }
        });

        processStream.getSideOutput(mary).print("mary");
        processStream.getSideOutput(bolb).print("bolb");
        processStream.print("other");
        env.execute();
    }
}

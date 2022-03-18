package top.damoncai.top.chapter03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import top.damoncai.top.bean.Event;
import top.damoncai.top.chapter02.Demo_03_Source_Customer;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/18 17:45
 */
public class Demo_11_Partition_Broadcast {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new Demo_03_Source_Customer.ClickSource());
        // 经广播后打印输出，并行度为 4
        stream. broadcast().print("broadcast").setParallelism(4);
        env.execute();
    }
}


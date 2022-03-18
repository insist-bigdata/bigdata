package top.damoncai.top.chapter03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class Demo_08_Partition_Shuffle {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new Demo_03_Source_Customer.ClickSource());
        // 经洗牌后打印输出，并行度为 4
        stream.shuffle().print("shuffle").setParallelism(4);
        env.execute();
    }
}


package top.damoncai.top.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import top.damoncai.top.bean.Event;

import java.time.Duration;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 14:09
 */
public class Demo_03_CoMap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3);
        DataStreamSource<Long> longStream = env.fromElements(1L, 2L, 3L);

        ConnectedStreams<Integer, Long> connectedStreams = intStream.connect(longStream);

        connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return integer + " : " + "Integer";
            }

            @Override
            public String map2(Long aLong) throws Exception {
                return aLong + " : " + "Long";
            }
        }).print();

        env.execute();
    }
}

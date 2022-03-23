package top.damoncai.top.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.damoncai.top.bean.Event;
import top.damoncai.top.bean.UrlViewCount;
import top.damoncai.top.chapter02.Demo_03_Source_Customer;

import java.sql.Timestamp;
import java.util.*;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 14:09
 */
public class Demo_04_TopN_ProcessAllWindowFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new Demo_03_Source_Customer.ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Event>() {
                                           @Override
                                           public long extractTimestamp(Event element, long recordTimestamp) {
                                               return element.timestamp;
                                           }
                                       }));

        // 转换数据格式
        stream
                .map(event -> event.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                        .process(new ProcessAllWindowFunction<String, List<Tuple2<String,Integer>>, TimeWindow>() {

                            @Override
                            public void process(ProcessAllWindowFunction<String, List<Tuple2<String, Integer>>, TimeWindow>.Context context, Iterable<String> iterable, Collector<List<Tuple2<String, Integer>>> out) throws Exception {
                                Map<String,Integer> res = new LinkedHashMap<>();
                                for (String url : iterable) {
                                    res.put(url,res.getOrDefault(url,0) + 1);
                                }
                                List<Tuple2<String,Integer>> list = new ArrayList();
                                //排序
                                for (String url : res.keySet()) {
                                    list.add(Tuple2.of(url,res.get(url)));
                                }
                                list.sort((e1,e2) -> e2.f1 - e1.f1);
                                out.collect(Arrays.asList(list.get(0),list.get(1)));
                            }
                        }).print();

        env.execute();
    }
}

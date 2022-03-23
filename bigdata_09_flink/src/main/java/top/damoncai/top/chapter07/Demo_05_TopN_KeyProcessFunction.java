package top.damoncai.top.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
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
public class Demo_05_TopN_KeyProcessFunction {

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
                .keyBy(url -> url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                    new AggregateFunction<String, Long, Long>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(String s, Long aLong) {
                            return aLong + 1L;
                        }

                        @Override
                        public Long getResult(Long aLong) {
                            return aLong;
                        }

                        @Override
                        public Long merge(Long aLong, Long acc1) {
                            return null;
                        }
                },
                    new ProcessWindowFunction<Long,UrlViewCount,String, TimeWindow>() {
                        @Override
                        public void process(String key, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
                            UrlViewCount urlViewCount = new UrlViewCount();
                            urlViewCount.url = key;
                            urlViewCount.count = iterable.iterator().next();
                            urlViewCount.windowStart = context.window().getStart();
                            urlViewCount.windowEnd = context.window().getEnd();
                            collector.collect(urlViewCount);
                        }
                    })
                .keyBy(data -> data.windowEnd)
                .process(
                    new KeyedProcessFunction<Long, UrlViewCount, String>() {

                        // 定义一个列表状态
                        private ListState<UrlViewCount> urlViewCountListState;

                        @Override
                        public void open(Configuration parameters) throws Exception {
                            // 从环境中获取列表状态句柄
                            urlViewCountListState = getRuntimeContext().getListState(
                                    new ListStateDescriptor<UrlViewCount>("url-view-count-list", Types.POJO(UrlViewCount.class)));
                        }

                        @Override
                        public void processElement(UrlViewCount urlViewCount, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> collector) throws Exception {
                            // 将 count 数据添加到列表状态中，保存起来
                            urlViewCountListState.add(urlViewCount);
                            // 注册 window end + 1ms 后的定时器，等待所有数据到齐开始排序
                            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
                        }

                        @Override
                        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String>
                                out) throws Exception {
                            // 将数据从列表状态变量中取出，放入 ArrayList，方便排序
                            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
                            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                                urlViewCountArrayList.add(urlViewCount);
                            }
                            // 清空状态，释放资源
                            urlViewCountListState.clear();
                            // 排序
                            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                                @Override
                                public int compare(UrlViewCount o1, UrlViewCount o2) {
                                    return o2.count.intValue() - o1.count.intValue();
                                }
                            });
                            // 取前两名，构建输出结果
                            StringBuilder result = new StringBuilder();
                            result.append("========================================\n");
                            result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
                            int len = urlViewCountArrayList.size() > 2 ? 2 : urlViewCountArrayList.size();
                            for (int i = 0; i < len; i++) {
                                UrlViewCount UrlViewCount = urlViewCountArrayList.get(i);
                                String info = "No." + (i + 1) + " "
                                        + "url：" + UrlViewCount.url + " "
                                        + "浏览量：" + UrlViewCount.count + "\n";
                                result.append(info);
                            }
                            result.append("========================================\n");
                            out.collect(result.toString());
                        }
                }).print("result:")

        ;
//                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                        .process(new ProcessAllWindowFunction<String, List<Tuple2<String,Integer>>, TimeWindow>() {
//
//                            @Override
//                            public void process(ProcessAllWindowFunction<String, List<Tuple2<String, Integer>>, TimeWindow>.Context context, Iterable<String> iterable, Collector<List<Tuple2<String, Integer>>> out) throws Exception {
//                                Map<String,Integer> res = new LinkedHashMap<>();
//                                for (String url : iterable) {
//                                    res.put(url,res.getOrDefault(url,0) + 1);
//                                }
//                                List<Tuple2<String,Integer>> list = new ArrayList();
//                                //排序
//                                for (String url : res.keySet()) {
//                                    list.add(Tuple2.of(url,res.get(url)));
//                                }
//                                list.sort((e1,e2) -> e2.f1 - e1.f1);
//                                out.collect(Arrays.asList(list.get(0),list.get(1)));
//                            }
//                        }).print();

        env.execute();
    }
}

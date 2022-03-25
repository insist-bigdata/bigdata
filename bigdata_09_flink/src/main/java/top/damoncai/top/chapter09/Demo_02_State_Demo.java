package top.damoncai.top.chapter09;

import akka.stream.impl.ReducerState;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
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
public class Demo_02_State_Demo {

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

                    ValueState<String> valueState;
                    ListState<String> listState;
                    MapState<String,Integer> mapState;
                    ReducingState<Event> reducerState;
                    AggregatingState<Event,String> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("valueState",String.class));
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("listState",String.class));
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("mapState",String.class,Integer.class));
                        reducerState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("reducerState",
                                new ReduceFunction<Event>() {
                                    @Override
                                    public Event reduce(Event event, Event t1) throws Exception {
                                        event.user = event.user + "~" + t1.user;
                                        return event;
                                    }
                                }
                                , Event.class));

                        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("aggregatingState",
                                new AggregateFunction<Event, Long, String>() {
                                    @Override
                                    public Long createAccumulator() {
                                        return 0L;
                                    }

                                    @Override
                                    public Long add(Event event, Long aLong) {
                                        return aLong ++;
                                    }

                                    @Override
                                    public String getResult(Long aLong) {
                                        return aLong + "~";
                                    }

                                    @Override
                                    public Long merge(Long aLong, Long acc1) {
                                        return null;
                                    }
                                }
                                , Long.class));

                    }

                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        valueState.update(event.user);
                        System.out.println("valueState: " + valueState.value());

                        listState.add(event.user);
                        System.out.println("listState" + listState.get());

                        mapState.put(event.user, mapState.get(event.user) == null ? 1 : (mapState.get(event.user) + 1));
                        System.out.println("mapState" + mapState.iterator());

                        reducerState.add(event);
                        System.out.println("reducerState" + reducerState.get());

                        aggregatingState.add(event);
                        System.out.println("aggregatingState" + aggregatingState.get());
                        System.out.println("==================================================");
                    }
                }).print();

        env.execute();
    }
}

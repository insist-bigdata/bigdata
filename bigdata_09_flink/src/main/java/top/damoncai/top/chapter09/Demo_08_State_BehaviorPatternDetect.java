package top.damoncai.top.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import top.damoncai.top.bean.Action;
import top.damoncai.top.bean.Event;
import top.damoncai.top.bean.Pattern;
import top.damoncai.top.chapter02.Demo_03_Source_Customer;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 14:09
 */
public class Demo_08_State_BehaviorPatternDetect {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取用户行为事件流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay")
                );
        // 定义行为模式流，代表了要检测的标准
        DataStreamSource<Pattern> patternStream = env.fromElements(
                        new Pattern("login", "pay"),
                        new Pattern("login", "buy")
                );

        // 定义广播状态的描述器，创建广播流
        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor("patternStream", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(bcStateDescriptor);

        actionStream.keyBy(data -> data.user)
                        .connect(broadcastStream)
                        .process(new BroadcastProcessFunction<Action, Pattern, String>() {

                            // 保存上一个状态
//                            private broadCase

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                getRuntimeContext().getMapState(new MapStateDescriptor("patternStream", Types.VOID, Types.POJO(Pattern.class)));
                            }

                            @Override
                            public void processElement(Action action, BroadcastProcessFunction<Action, Pattern, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {

                            }

                            @Override
                            public void processBroadcastElement(Pattern pattern, BroadcastProcessFunction<Action, Pattern, String>.Context context, Collector<String> collector) throws Exception {
                                // 更新广播数据

                            }
                        }).print();
        env.execute();
    }
}

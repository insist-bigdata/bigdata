package top.damoncai.top.chapter02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import top.damoncai.top.bean.Event;

import java.util.Random;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/18 11:28
 */
public class Demo_04_Source_Customer_Parallel {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream = env.addSource(new CustomSource()).setParallelism(2);

        stream.print();

        env.execute();
    }
}

class CustomSource implements ParallelSourceFunction<Integer>
{
    private boolean running = true;
    private Random random = new Random();
    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(random.nextInt());
        }
    }
    @Override
    public void cancel() {
        running = false;
    }
}

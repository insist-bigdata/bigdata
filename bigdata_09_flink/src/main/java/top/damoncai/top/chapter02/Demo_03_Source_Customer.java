package top.damoncai.top.chapter02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import top.damoncai.top.bean.Event;

import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/18 10:42
 */
public class Demo_03_Source_Customer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print();

        env.execute();
    }

    public static class ClickSource implements SourceFunction<Event> {
        // 声明一个布尔变量，作为控制数据生成的标识位
        private Boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random(); // 在指定的数据集中随机选取数据
            String[] users = {"Mary", "Alice", "Bob", "Cary"};
            String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

            while (running) {
                Event event = new Event(
                        users[random.nextInt(users.length)],
                        urls[random.nextInt(urls.length)],
                        Calendar.getInstance().getTimeInMillis());
//                System.out.println("生成数据：" + event);
                ctx.collect(event);
                // 隔 2 秒生成一个点击事件，方便观测
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}


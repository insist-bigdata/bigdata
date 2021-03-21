package top.damoncai.hadoop.mapreduce.demo_06_comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/11 13:59
 */
public class FlowReducer extends Reducer<FlowBean, Text,Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();
    private Long upFlow;
    private Long downFlow;

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}

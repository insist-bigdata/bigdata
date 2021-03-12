package top.damoncai.hadoop.mapreduce.demo_07_partitioncomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/11 13:59
 */

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean flowBeanKey = new FlowBean();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");

        String phone = splits[0];

        flowBeanKey.setUpFlow(Long.valueOf(splits[1]));
        flowBeanKey.setDownFlow(Long.valueOf(splits[2]));
        flowBeanKey.setTotalFlow();

        v.set(phone);

        context.write(flowBeanKey,v);

    }
}

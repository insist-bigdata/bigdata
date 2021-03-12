package top.damoncai.hadoop.mapreduce.demo_03_writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/11 13:59
 */

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    private FlowBean flowBean = new FlowBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");

        //电话
        String phone = splits[1];

        //上行流量
        Long upFlow = Long.valueOf(splits[splits.length - 3]);

        //下行流量
        Long downFlow = Long.valueOf(splits[splits.length - 2]);

        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setTotalFlow();

        k.set(phone);

        context.write(k,flowBean);

    }
}

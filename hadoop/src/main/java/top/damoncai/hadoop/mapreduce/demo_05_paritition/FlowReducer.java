package top.damoncai.hadoop.mapreduce.demo_05_paritition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/11 13:59
 */

public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();
    private Long upFlow;
    private Long downFlow;

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        upFlow = 0L;
        downFlow = 0L;
        for (FlowBean flowBean : values) {
            upFlow += flowBean.getUpFlow();
            downFlow += flowBean.getDownFlow();
        }
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setTotalFlow();

        context.write(key,flowBean);
    }
}

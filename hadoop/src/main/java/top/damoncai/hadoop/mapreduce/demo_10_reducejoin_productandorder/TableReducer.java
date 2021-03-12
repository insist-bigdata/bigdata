package top.damoncai.hadoop.mapreduce.demo_10_reducejoin_productandorder;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhishun.cai
 * @date 2021/3/12 11:14
 */

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        List<TableBean> orders = new ArrayList<>();
        TableBean tb= null;

        for (TableBean tableBean : values) {
            String flag = tableBean.getFlag();
            if (flag.contains("order")) {
                orders.add(JSON.parseObject(JSON.toJSONString(tableBean),TableBean.class));
            }else {
                tb = JSON.parseObject(JSON.toJSONString(tableBean),TableBean.class);
            }
        }

        for (TableBean order : orders) {
            order.setPname(tb.getPname());
            context.write(order,NullWritable.get());
        }
    }
}

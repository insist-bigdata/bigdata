package top.damoncai.hbase.c02_customer_hbase_mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/10/18 15:07
 */

public class FruitMapper extends Mapper<LongWritable, Text,LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}

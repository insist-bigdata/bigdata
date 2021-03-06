package top.damoncai.hbase.c02_customer_hbase_mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/10/19 14:33
 */

public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 1.遍历 100 Apple   red
        for (Text value: values) {
            String[] fields = value.toString().split("\t");
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(fields[2]));
            context.write(NullWritable.get(),put);
        }
    }
}

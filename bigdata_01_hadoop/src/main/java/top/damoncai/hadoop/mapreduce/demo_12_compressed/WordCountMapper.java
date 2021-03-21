package top.damoncai.hadoop.mapreduce.demo_12_compressed;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/11 10:45
 */

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 切割每一行数据
        String[] books = value.toString().split(" ");
        // 输出
        for (String book : books) {
            k.set(book);
            context.write(k,v);
        }
    }
}

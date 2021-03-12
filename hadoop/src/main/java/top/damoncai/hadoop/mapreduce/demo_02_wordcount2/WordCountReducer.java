package top.damoncai.hadoop.mapreduce.demo_02_wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/11 10:45
 */

public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    private IntWritable num = new IntWritable();

    private int sum;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       sum = 0;
        for (IntWritable value : values) {
            sum += 1;
        }
        this.num.set(sum);
        context.write(key,num);
    }
}

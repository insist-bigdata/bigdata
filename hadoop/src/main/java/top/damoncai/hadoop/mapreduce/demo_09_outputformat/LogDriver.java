package top.damoncai.hadoop.mapreduce.demo_09_outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/11 22:18
 */

public class LogDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置自定义的 outputformat
        job.setOutputFormatClass(LogOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\11_input\\inputoutputformat"));
        // 虽 然 我 们 自 定 义 了 outputformat ， 但 是 因 为 我 们 的 outputformat 继承自fileoutputformat
        //而 fileoutputformat 要输出一个_SUCCESS 文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("D:\\_output\\inputoutputformat"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

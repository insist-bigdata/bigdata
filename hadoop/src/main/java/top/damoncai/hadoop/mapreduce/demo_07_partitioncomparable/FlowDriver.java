package top.damoncai.hadoop.mapreduce.demo_07_partitioncomparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/11 14:12
 */

public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取 job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 关联本 Driver 程序的 jar
        job.setJarByClass(FlowDriver.class);
        // 3 关联 Mapper 和 Reducer 的 jar
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 4 设置 Mapper 输出的 kv 类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        // 5 设置最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //8 指定自定义分区器
        job.setPartitionerClass(CustomerPartitioner.class);
        //9 同时指定相应数量的 ReduceTask
        job.setNumReduceTasks(5);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\_output\\writable\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\_output\\partitionercomparable"));
        // 7 提交 job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

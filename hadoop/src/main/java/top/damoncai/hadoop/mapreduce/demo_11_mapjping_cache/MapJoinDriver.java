package top.damoncai.hadoop.mapreduce.demo_11_mapjping_cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @author zhishun.cai
 * @date 2021/3/12 14:13
 */

public class MapJoinDriver {

    public static void main(String[] args) throws Exception {
        // 1 获取 job 信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 设置加载 jar 包路径
        job.setJarByClass(MapJoinDriver.class);
        // 3 关联 mapper
        job.setMapperClass(MapJoinMapper.class);
        // 4 设置 Map 输出 KV 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出 KV 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 加载缓存数据
        job.addCacheFile(new URI("file:///D:/11_input/tablecache/pd.txt"));
        // Map 端 Join 的逻辑不需要 Reduce 阶段，设置 reduceTask 数量为 0
        job.setNumReduceTasks(0);
        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\11_input\\inputtable2"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\_output\\cache"));
        // 7 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

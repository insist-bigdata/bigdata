package top.damoncai.hbase.c02_customer_hbase_mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author zhishun.cai
 * @date 2021/10/19 15:13
 */

public class FruitDriver implements Tool {

    Configuration configuration = null;

    public int run(String[] args) throws Exception {
        // 1.获取Job对象
        Job job = Job.getInstance(configuration);

        // 2. 设置驱动类路径
        job.setJarByClass(FruitDriver.class);

        // 3.设置Mapper和Mapper输出的KV类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 4.设置Reducer类
        TableMapReduceUtil.initTableReducerJob(args[1], FruitReducer.class,job);

        // 5.设置输入参数
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 6.提交任务
        boolean res = job.waitForCompletion(true);

        return res ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            int run = ToolRunner.run(conf, new FruitDriver(), args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package top.damoncai.hbase.c03_customer_hbase_mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/10/19 15:13
 */

public class FruitDriver implements Tool {

    Configuration configuration = null;

    public int run(String[] args) throws Exception {
        //得到 Configuration
        Configuration conf = this.getConf();
        //创建 Job 任务
        Job job = Job.getInstance(conf,
                this.getClass().getSimpleName());
        job.setJarByClass(FruitDriver.class);
        //配置 Job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);
        //设置 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者 是老版本
        TableMapReduceUtil.initTableMapperJob(
                args[0], //数据源的表名
                scan, //scan 扫描控制器
                FruitMapper.class,//设置 Mapper 类
                ImmutableBytesWritable.class,//设置 Mapper 输出 key 类型
                Put.class,//设置 Mapper 输出 value 值类型
                job//设置给哪个 JOB
        );
        //设置 Reducer
        TableMapReduceUtil.initTableReducerJob(args[1],
                FruitReducer.class, job);
        //设置 Reduce 数量，最少 1 个
        job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw new IOException("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
//        Configuration conf = new Configuration();
        Configuration conf = HBaseConfiguration.create();
        try {
            int run = ToolRunner.run(conf, new FruitDriver(),new String[]{"fruit1","fruit3"});
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

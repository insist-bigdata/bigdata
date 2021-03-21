package top.damoncai.hadoop.mapreduce.demo_11_mapjping_cache;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import top.damoncai.hadoop.mapreduce.demo_10_reducejoin_productandorder.TableBean;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhishun.cai
 * @date 2021/3/12 14:08
 */

public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Map<String, String> pdMap = new HashMap<>();
    private Text text = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);
        //通过包装流转换为 reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //切割一行
            //01 小米
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }
        //关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取大表数据
        //1001 01 1
        String[] fields = value.toString().split("\t");
        //通过大表每行数据的 pid,去 pdMap 里面取出 pname
        String pname = pdMap.get(fields[1]);
        //将大表每行数据的 pid 替换为 pname
        text.set(fields[0] + "\t" + pname + "\t" + fields[2]);
        //写出
        context.write(text,NullWritable.get());
    }
}

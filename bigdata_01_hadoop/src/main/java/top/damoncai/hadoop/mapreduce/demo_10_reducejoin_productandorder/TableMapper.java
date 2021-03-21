package top.damoncai.hadoop.mapreduce.demo_10_reducejoin_productandorder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/12 11:03
 */

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    private String filename;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取对应文件名称
        InputSplit split = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) split;
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        if(filename.contains("order")) { //order
            outK.set(splits[1]);
            outV.setId(splits[0]);
            outV.setPid(splits[1]);
            outV.setAmount(Integer.valueOf(splits[2]));
            outV.setPname("");
            outV.setFlag("order");
        }else{//pd
            outK.set(splits[0]);
            outV.setId("");
            outV.setPid(splits[0]);
            outV.setAmount(0);
            outV.setPname(splits[1]);
            outV.setFlag("pd");
        }
        context.write(outK,outV);
    }
}

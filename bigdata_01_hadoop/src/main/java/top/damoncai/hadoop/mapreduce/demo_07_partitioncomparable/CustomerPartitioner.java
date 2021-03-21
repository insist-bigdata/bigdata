package top.damoncai.hadoop.mapreduce.demo_07_partitioncomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zhishun.cai
 * @date 2021/3/11 18:25
 */

public class CustomerPartitioner extends Partitioner<FlowBean,Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String perfix = text.toString().substring(0,3);
        int num;
        switch (perfix) {
            case "137":
                num = 0;
                break;
            case "138":
                num = 1;
                break;
            case "139":
                num = 2;
                break;
            case "135":
                num = 3;
                break;
            default:
                num = 4;
        }
        return num;
    }
}

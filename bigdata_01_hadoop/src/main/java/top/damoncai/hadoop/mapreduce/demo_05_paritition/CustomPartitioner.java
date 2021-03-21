package top.damoncai.hadoop.mapreduce.demo_05_paritition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zhishun.cai
 * @date 2021/3/11 16:48
 */

public class CustomPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        int num;
        String perfix = text.toString().substring(0,3);
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

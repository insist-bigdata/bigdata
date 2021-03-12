package top.damoncai.hadoop.mapreduce.demo_07_partitioncomparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/3/11 13:54
 */

public class FlowBean implements Writable, WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long totalFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    public void setTotalFlow() {
        this.totalFlow = this.upFlow + this.downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(totalFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.totalFlow = dataInput.readLong();
    }

    @Override
    public int compareTo(FlowBean flowBean) {
        return this.getTotalFlow() > flowBean.getTotalFlow() ? -1 : 1;
    }
}

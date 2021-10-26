package top.damoncai.hbase.c03_customer_hbase_mr2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/10/21 9:38
 */

public class FruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // key就是rowkey

        Put put = new Put(key.get());

        for (Cell cell : value.rawCells()) {
            put.add(cell);
        }

        context.write(key,put);
    }
}

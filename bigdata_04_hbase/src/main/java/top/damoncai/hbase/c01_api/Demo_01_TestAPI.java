package top.damoncai.hbase.c01_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zhishun.cai
 * @date 2021/10/14 9:41
 */

public class Demo_01_TestAPI {

    private Admin admin;

    private Connection connection;


    @Before
    public void before() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.111.61,192.168.111.62,192.168.111.63");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    @After
    public void after() throws IOException {
        if(null != connection) {
            connection.close();
        }

        if(null != admin) {
            admin.close();
        }
    }

    /**
     * 判断表名是否存在
     * @throws IOException
     */
    @Test
    public void isExistTable() throws IOException {
        String tableName = "bigdata:teacher";
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        System.out.println(exists);
    }

    /**
     * 创建表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        String tableName = "bigdata:teacher";
        // 判断表名是否存在
        if(admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("表名已存在");
            return;
        }
        // 创建表描述器并设置表名
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 设置列族
        HColumnDescriptor columnDescriptorF1 = new HColumnDescriptor("f1");
        HColumnDescriptor columnDescriptorF2 = new HColumnDescriptor("f2");
        tableDescriptor.addFamily(columnDescriptorF1).addFamily(columnDescriptorF2);
        // 设置列族
        admin.createTable(tableDescriptor);
        System.out.println("表创建成功");
    }

    /**
     * 删除表
     */
    @Test
    public void deleteTable() throws IOException {
        TableName tableName = TableName.valueOf("bigdata:teacher");
        // 判断表名是否存在
        if(!admin.tableExists(tableName)) {
            System.out.println("表名不存在");
            return;
        }
        // 使表下线
        admin.disableTable(tableName);
        // 删除表
        admin.deleteTable(tableName );
        System.out.println("删除成功");
    }

    /**
     * 创建命名空间
     */
    @Test
    public void createNameSpace() {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("bigdata_1").build();
        try{
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e) {
            System.out.println("命名空间已存在");
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功");
    }

    /**
     * 插入数据
     */
    @Test
    public void insertData() throws IOException {
        TableName tableName = TableName.valueOf("bigdata:teacher");
        // 判断表名是否存在
        if(!admin.tableExists(tableName)) {
            System.out.println("表名不存在");
            return;
        }
        Table table = connection.getTable(tableName);

        Put put = new Put("100".getBytes());
        put.addColumn("f1".getBytes(),"name".getBytes(),"damon".getBytes());
        put.addColumn("f2".getBytes(),"sex".getBytes(),"male".getBytes());

        table.put(put);

        System.out.println("插入成功");
        table.close();
    }

    /**
     * 获取数据
     */
    @Test
    public void getData() throws IOException {
        TableName tableName = TableName.valueOf("bigdata:teacher");
        // 判断表名是否存在
        if(!admin.tableExists(tableName)) {
            System.out.println("表名不存在");
            return;
        }
        Table table = connection.getTable(tableName);

        Get get = new Get(Bytes.toBytes("100")); // 设置rowKey

//        get.addFamily(Bytes.toBytes("f1"));// 设置列族

        get.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("sex")); // 设置列族和列明

        Result result = table.get(get);

        for (Cell cell : result.rawCells()) {
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String name = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(row + " " + family + " " + name + " " + value);
        }

        table.close();
    }

    /**
     * 获取数据Scanner
     * @throws IOException
     */
    @Test
    public void scanData() throws IOException {
        TableName tableName = TableName.valueOf("bigdata:teacher");
        // 判断表名是否存在
        if(!admin.tableExists(tableName)) {
            System.out.println("表名不存在");
            return;
        }
        Table table = connection.getTable(tableName);

        // 左开右闭
        Scan scan = new Scan(Bytes.toBytes("100"),Bytes.toBytes("102"));

        ResultScanner rs = table.getScanner(scan);

        for (Result r : rs) {
            for (Cell cell : r.rawCells()) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String name = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(row + " " + family + " " + name + " " + value);
            }
        }
        table.close();
    }

    @Test
    public void delete() throws IOException {
        TableName tableName = TableName.valueOf("bigdata:teacher");
        // 判断表名是否存在
        if(!admin.tableExists(tableName)) {
            System.out.println("表名不存在");
            return;
        }
        Table table = connection.getTable(tableName);

        //构建删除对象
        Delete delete = new Delete(Bytes.toBytes("100"));
        delete.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"));
        table.delete(delete);
    }
}

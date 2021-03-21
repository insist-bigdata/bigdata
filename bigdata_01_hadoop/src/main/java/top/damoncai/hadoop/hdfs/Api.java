package top.damoncai.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.kerby.config.Conf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author zhishun.cai
 * @date 2021/3/10 15:02
 */

public class Api {

    private FileSystem fs;

    @Before
    public void init() throws Exception {
        URI uri = new URI("hdfs://hadoop01:8020");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        String user = "root";
        fs = FileSystem.get(uri,configuration,user);
    }

    @After
    public void after() throws Exception {
        if(null != fs) fs.close();
    }

    /**
     * 创建文件夹
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        fs.mkdirs(new Path("/xiyouji/huaguoshan"));
    }

    /**
     * 上传文件
     * 参数优先级
     * hdfs-default.xml => hdfs-site.xml => 在项目资源目录下配置文件 => 代码里配置
     * @throws Exception
     */
    @Test
    public void upload() throws Exception {
        /**
         * 参数一：是否删除本地文件
         * 参数二：文件存在是否覆盖
         * 参数三：上传文件原路径
         * 参数四：上传文件目标路径
         */
        fs.copyFromLocalFile(true,true,new Path("D:\\swk.txt"),new Path("/xiyouji/huaguoshan"));
        fs.mkdirs(new Path("/xiyouji/huaguoshan"));
    }

    /**
     * 文件下载
     */
    @Test
    public void download() throws IOException {
        /**
         * 参数一：原文件是否删除`
         * 参数二：原文件路径HDFS
         * 参数三：目标地址路径
         * 参数四：是开开启crc校验
         */
        fs.copyToLocalFile(false,new Path("/xiyouji/huaguoshan/swk.txt"),new Path("D:\\"),false);
    }

    /**
     * 文件删除
     */
    @Test
    public void delete() throws IOException {
        /**
         * 参数一：需要删除的路径`
         * 参数二：是否递归删除
         */
        fs.delete(new Path("/sanguo"),true);
    }

    /**
     * 更名和移动
     */
    @Test
    public void rename() throws IOException {
        fs.rename(new Path("/input/word.txt"),new Path("/input/word2.txt"));
    }

    /**
     * 文件详情
     */
    @Test
    public void fileDetail() throws Exception {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    /**
     * 更名和移动
     */
    @Test
    public void fileOrDir() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.isFile() ? "文件" : "文件夹");
        }
    }
}

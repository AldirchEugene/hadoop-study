package com.ae.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    private static final String uri = "hdfs://hadoop102:9820";
    FileSystem fs;


    /**
     * 上传文件
     * *@param delSrc:是否删除源路径文件
     * *@param overwrite:是否覆盖已存在的文件
     * *@param src path:源路径
     * *@param dst path:目标路径
     */
    @Test
    public void testCopyFromLocalFile() throws IOException {
        fs.copyFromLocalFile(false,true,
                new Path("F:\\zhoudong\\workspace\\hadoop_env\\upload_test\\hello.txt"),
                new Path("/"));
    }

    /**
     * 下载文件
     * *@param delSrc: 是否删除源文件
     * *@param src path:源文件路径
     * *@param dst path:目标路径
     * *@param useRawLocalFileSystem:是否开启文件校验
     */
    @Test
    public void testCopyToLocalFile() throws IOException {
        fs.copyToLocalFile(false,
                new Path("/hello.txt"),
                new Path("F:\\zhoudong\\workspace\\hadoop_env\\upload_test\\hello2.txt"),
                false);
    }

    /**
     * 删除文件和目录
     * * @param f:要删除的路径
     * * @param recursive:path为目录，设置为为true，则目录被删除，否则将引发异常。在文件的递归情况可以设置为true或false。
     */
    @Test
    public void testDelete() throws IOException {
        fs.delete(new Path("/weiguo/"),true);
    }

    /**
     * 文件移到或更名
     * * @param src:要重命名的路径
     * * @param dst:重命名后的新路径
     */
    @Test
    public void testRename() throws IOException {
        fs.rename(new Path("/hello.txt"),
                new Path("/hello2.txt"));
    }


    /**
     * 文件详情查看
     * *@param f:路径
     * *@param recursive:如果子目录需要递归地遍历
     */
    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        while (iterator.hasNext()){
            LocatedFileStatus status = iterator.next();
            // 文件名称
            System.out.println("文件名称:"+status.getPath().getName());
            // 大小
            System.out.println("大小:"+status.getLen());
            // 权限
            System.out.println("权限:"+status.getPermission());
            // 分组
            System.out.println("分组:"+status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("主机节点:"+host);
                }
            }
        }
    }

    /**
     * 文件或文件夹判断
     * * @param f:路径
     */
    @Test
    public void testListStatus() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isDirectory()){
                System.out.println("文件夹:"+fileStatus.getPath().getName());
            }else {
                System.out.println("文件:"+fileStatus.getPath().getName());
            }
        }
    }

    /**
     * 创建hdfs客户端连接
     */
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI(HdfsClient.uri);
        Configuration conf = new Configuration();
        conf.set("dfs.replication","3");
        fs = FileSystem.get(uri, conf,"root");
    }

    /**
     * 关闭hdfs客户端连接
     */
    @After
    public void close() throws IOException {
        fs.close();
    }
}

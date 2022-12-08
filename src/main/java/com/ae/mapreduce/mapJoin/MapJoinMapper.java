package com.ae.mapreduce.mapJoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * 1. 处理缓存文件：将job中设置的缓存路径获取到
 * 2. 根据缓存路径再结合输入流把pd.txt 内容写入内存的容器中维护
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> pdMap = new HashMap<String, String>();

    private Text outk = new Text();

    /**
     * 处理缓存文件
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 将job中设置的缓存路径获取到
        URI[] cacheFiles = context.getCacheFiles();
        URI cacheFile = cacheFiles[0];
        // 准备输入流对象
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream pdInPut = fs.open(new Path(cacheFile));
        // 通过流对象将数据读入 保存到内存的Map中
        BufferedReader reader = new BufferedReader(new InputStreamReader(pdInPut, "UTF-8"));
        // 按行读入
        String line;
        while ((line = reader.readLine()) != null){
            // 将数据保存到Map中 01	小米
            String[] datas = line.split("\t");
            pdMap.put(datas[0], datas[1]);
        }

        //关闭资源
        IOUtils.closeStream(pdInPut);
    }

    /**
     * 处理mapjoin的逻辑
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取当前行数据 1001	01	1
        String lineData = value.toString();
        // 切割
        String[] orderDatas = lineData.split("\t");
        // 进行数据关联获取pname
        String pname = pdMap.get(orderDatas[1]);
        // 封装输出结果
        String result = orderDatas[0] + "\t" + pname + "\t" + orderDatas[2];
        outk.set(result);
        // 将结果写出
        context.write(outk, NullWritable.get());

    }
}

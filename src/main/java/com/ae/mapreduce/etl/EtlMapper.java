package com.ae.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 1. 处理缓存文件：将job中设置的缓存路径获取到
 * 2. 根据缓存路径再结合输入流把pd.txt 内容写入内存的容器中维护
 */
public class EtlMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    
    private Text outk = new Text();

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

        // 获取当前行数据
        String line = value.toString();
        // 切割
        String[] datas = line.split(" ");
        // 遍历集合 将字段长度 小于等于11的过滤掉
        for (String data : datas) {
            if(data.length() > 11){
                outk.set(data);
                context.write(outk, NullWritable.get());
            }else {
                return;
            }
        }
    }
}

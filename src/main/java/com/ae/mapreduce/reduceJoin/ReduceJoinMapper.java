package com.ae.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Orderpd> {

    private Text outk = new Text();
    private Orderpd outv = new Orderpd();
    private FileSplit inputSplit;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       inputSplit = (FileSplit) context.getInputSplit();
    }

    /**
     * 业务处理方法--> 将两个需要做关联的文件数据进行搜集
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
        // 切割数据
        String[] datas = line.split("\t");
        // 将当前数据封装到 Orderpd中
        if (inputSplit.getPath().getName().contains("order")) {
            // 当前数据来源于order.txt 文件  1001	01	1
            // 封装输出数据的key
            outk.set(datas[1]);
            // 封装输出数据的value
            outv.setOrderId(datas[0]);
            outv.setPid(datas[1]);
            outv.setAmount(Integer.parseInt(datas[2]));
            outv.setPname("");
            outv.setTitle("order");
        }else {
            // 当前数据来源于 pd.txt   01	小米
            // 封装输出数据的key
            outk.set(datas[0]);
            // 封装输出数据的value
            outv.setOrderId("");
            outv.setPid(datas[0]);
            outv.setAmount(0);
            outv.setPname(datas[1]);
            outv.setTitle("pd");
        }

        // 将数据写出
        context.write(outk, outv);
    }
}

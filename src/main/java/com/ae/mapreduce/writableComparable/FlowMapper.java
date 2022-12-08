package com.ae.mapreduce.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行数据
        // 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String line = value.toString();

        // 按照指定的格式截取数据
        String[] phoneData = line.split("\t");

        // 封装输出的key
        outK.setUpFlow(Integer.valueOf(phoneData[phoneData.length - 3]));
        outK.setDownFlow(Integer.valueOf(phoneData[phoneData.length - 2]));
        outK.setSumFlow();

        // 封装输出的value
        outV.set(phoneData[1]);

        // 写出
        context.write(outK,outV);
    }
}

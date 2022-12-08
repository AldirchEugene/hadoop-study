package com.ae.mapreduce.wordcount1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 以WordCount案例 为例：
 * 自定义的Mapper类 需要继承Hadoop提供的Mapper 并且根据具体业务指定输入数据和输出数据的数据类型
 *
 * 输入数据的类型:
 * KEYIN:   读取文件的偏移量  数字（LongWritable）
 * VALUEIN: 读取文件的一行数据  文本（Text）
 *
 * 输出数据的类型:
 * KEYOUT:   输出数据的key的类型 就是一个单词（Text）
 * VALUEOUT: 输出数据value的类型 给单词的标记 1 数字（IntWritable）
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    /**
     * 在任务开始时调用一次。
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    /**
     * Map阶段的核心业务处理方法，每输入一行数据会调用一次map方法
     * @param key 输入数据的key
     * @param value 输入数据的value
     * @param context 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取当前输入的一行数据
        String line = value.toString();

        // 按照指定数据规则切割数据【我以空格切割】
        String[] words = line.split(" ");

        // 遍历数组,封装输出数据的key和value
        for (String word : words) {
            outK.set(word);
            context.write(outK,outV);
        }
    }

    /**
     * 在任务结束时调用一次。
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}

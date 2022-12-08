package com.ae.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 以WordCount案例 为例：
 * 自定义的Reducer类 需要继承Hadoop提供的Reducer 并且根据具体业务指定输入数据和输出数据的数据类型
 *
 * 输入数据的类型:
 * KEYIN:   Map端输出的key的数据类型
 * VALUEIN: Map端输出的value的数据类型
 *
 * 输出数据的类型
 * KEYOUT:   输出数据的key的类型 就是一个单词（Text）
 * VALUEOUT: 输出数据value的类型 单词出现的总次数（IntWritable）
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable();

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
     *  Reduce阶段的核心业务处理方法，一组相同key的values会调用一次reduce方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // 遍历values
        for (IntWritable value : values) {
            // 对value累加进行累加 输出结果
            sum += value.get();
        }

        // 封装key和value
        outK.set(key);
        outV.set(sum);

        // 写出
        context.write(outK,outV);
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

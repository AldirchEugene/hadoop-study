package com.ae.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义Combiner，需要继承Hadoop提供的Reduce类
 * TODO 注意：虽然自定义的Combiner类继承了Reducer类，但Combiner流程一定发生在Map阶段
 */
public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable();

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
}

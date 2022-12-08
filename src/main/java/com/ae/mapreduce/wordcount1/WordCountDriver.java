package com.ae.mapreduce.wordcount1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MR程序的驱动类：主要用于提交MR任务
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1、声明配置对象
        Configuration conf = new Configuration();

        // 设置任务提交的队列为指定的队列hive
        //conf.set("mapreduce.job.queuename","hive");

        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec");

        // 开启reduce端输出压缩
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        // 设置reduce端输出压缩方式
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec");

        // 2、声明Job对象
        Job job = Job.getInstance(conf);

        // 3、指定当前Job的驱动类
        job.setJarByClass(WordCountDriver.class);

        // 4、指定当前Job的 Mapper 和 Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 5、指定Map阶段输出数据的 key 和 value 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 6、指定最终输出结果的 key 和 value 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 7、指定输入数据的目录和输出数据的目录【本地测试使用】
        FileInputFormat.setInputPaths(job, new Path("F:\\zhoudong\\workspace\\hadoop_env\\test_data\\in\\wcinput"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\zhoudong\\workspace\\hadoop_env\\test_data\\out\\wcoutput7"));

        // 7、指定输入数据的目录和输出数据的目录【集群测试使用】
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8、提交Job
        job.waitForCompletion(true);
    }
}

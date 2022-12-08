package com.ae.mapreduce.outPutFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * MR程序的驱动类：主要用于提交MR任务
 */
public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1、声明配置对象
        Configuration conf = new Configuration();

        // 2、声明Job对象
        Job job = Job.getInstance(conf);

        // 3、指定当前Job的驱动类
        job.setJarByClass(LogDriver.class);

        // 4、指定当前Job的 Mapper 和 Reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        // 5、指定Map阶段输出数据的 key 和 value 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 6、指定最终输出结果的 key 和 value 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定自定义的LogOutPutFormat
        job.setOutputFormatClass(LogOutPutFormat.class);

        // 7、指定输入数据的目录和输出数据的目录【本地测试使用】
        FileInputFormat.setInputPaths(job, new Path("F:\\zhoudong\\workspace\\hadoop_env\\test_data\\in\\outputformat"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\zhoudong\\workspace\\hadoop_env\\test_data\\out\\outputformat"));

        // 7、指定输入数据的目录和输出数据的目录【集群测试使用】
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8、提交Job
        job.waitForCompletion(true);
    }
}

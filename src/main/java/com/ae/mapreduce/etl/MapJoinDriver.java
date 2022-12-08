package com.ae.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(EtlMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置Reduce的数量为0
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("F:\\zhoudong\\workspace\\hadoop_env\\test_data\\in\\ETL"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\zhoudong\\workspace\\hadoop_env\\test_data\\out\\ETL"));

        job.waitForCompletion(true);
    }
}

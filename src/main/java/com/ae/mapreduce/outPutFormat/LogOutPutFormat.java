package com.ae.mapreduce.outPutFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * 自定义LogOutPutFormat类，需要继承Hadoop提供的OutPutFormat类，并实现getRecordWriter方法
 */
public class LogOutPutFormat extends FileOutputFormat<Text, NullWritable> {

    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        LogRecordWriter lrw = new LogRecordWriter(job);
        return lrw;
    }
}

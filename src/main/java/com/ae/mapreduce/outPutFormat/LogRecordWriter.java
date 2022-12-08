package com.ae.mapreduce.outPutFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * 自定义LogRecordWriter，需要实现Hadoop的RecordWriter类
 */
public class LogRecordWriter extends RecordWriter {

    // 定义输出路径
    private String hikariPath = "F:\\zhoudong\\workspace\\hadoop_env\\test_data\\out\\outputformat2\\hikari.log";
    private String otherPath = "F:\\zhoudong\\workspace\\hadoop_env\\test_data\\out\\outputformat2\\other.log";

    // 定义输出流
    private FSDataOutputStream hikariPathOut;
    private FSDataOutputStream otherPathOut;

    public LogRecordWriter(TaskAttemptContext job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        hikariPathOut = fs.create(new Path(hikariPath));
        otherPathOut = fs.create(new Path(otherPath));
    }

    public void write(Object key, Object value) throws IOException, InterruptedException {
        String str = key.toString();
        if(str.contains("hikari")){
            hikariPathOut.writeBytes(str + "\n");
        }else {
            otherPathOut.writeBytes(str + "\n");
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(hikariPathOut,otherPathOut);
    }
}

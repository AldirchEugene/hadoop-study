package com.ae.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import java.io.*;

public class CompressTest {

    public static void main(String[] args) throws IOException {
        //compress("F:\\zhoudong\\workspace\\hadoop_env\\test_data\\in\\jianai\\test.txt","org.apache.hadoop.io.compress.DefaultCodec");
        decompress("F:\\zhoudong\\workspace\\hadoop_env\\test_data\\in\\jianai\\test.txt.deflate");
    }

    //压缩
    private static void compress(String filename, String method) throws IOException {
        //1 获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));

        //2 获取输出流
        //获取压缩编解码器codec
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodecByName(method);

        //获取普通输出流,文件后面需要加上压缩后缀
        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));
        //获取压缩输出流,用压缩解码器对fos进行压缩
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //3 流的对拷
        IOUtils.copyBytes(fis,cos,new Configuration());

        //4 关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    //解压缩
    private static void decompress(String filename) throws IOException {
        //校验是否能解压缩
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null) {
            System.out.println("cannot find codec for file " + filename);
            return;
        }

        //1 获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));
        CompressionInputStream cis = codec.createInputStream(fis);

        //2 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("F:\\zhoudong\\workspace\\hadoop_env\\test_data\\in\\jianai\\test.txt"));

        //3 流的对拷
        IOUtils.copyBytes(cis,fos,new Configuration());

        //4 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }
}

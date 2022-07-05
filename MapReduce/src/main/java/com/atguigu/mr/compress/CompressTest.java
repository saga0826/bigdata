package com.atguigu.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.*;

/**
 * 对文件进行压缩和解压缩
 */
public class CompressTest {
    //原始文件路径
    String srcFile = "E:\\temp\\bigdata\\mr\\out\\ja.txt.deflate";
    //输入文件路径
    String destFile = "E:\\temp\\bigdata\\mr\\out\\ja.txt";
    /**
     * 压缩：通过一个能够具备压缩功能的输出流写出文件
     */
    @Test
    public void testPress() throws IOException, ClassNotFoundException {
        //声明一个输入流
        FileInputStream in = new FileInputStream(new File(srcFile));
        Configuration conf = new Configuration();
        String classPath = "org.apache.hadoop.io.compress.DefaultCodec";
        Class<?> codecClass = Class.forName(classPath);
        // 获取一个编解码器（压缩工具对象）
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        // 声明一个输出流，将文件写出
        FileOutputStream out = new FileOutputStream(new File(destFile + codec.getDefaultExtension()));
        // 把普通的输出流让 CompressionCodec 包装一下
        CompressionOutputStream outputStream = codec.createOutputStream(out);

        // 读写数据
        IOUtils.copyBytes(in, outputStream, conf);

        // 关闭流
        IOUtils.closeStream(in);
        IOUtils.closeStream(outputStream);
    }


    /**
     * 解压缩 通过一个能够具备解压缩功能的输出流写出文件
     */
    @Test
    public void testCodecCompress() throws IOException, ClassNotFoundException {

        //声明一个输入流
        FileInputStream in = new FileInputStream(new File(srcFile));
        Configuration conf = new Configuration();
        // 获取一个编解码器（解压缩工具对象）
        CompressionCodec codec =
                new CompressionCodecFactory(conf).getCodec(new Path(srcFile));
        // 把普通的输入流让 CompressionCodec 包装一下
        CompressionInputStream inputStream = codec.createInputStream(in);
        // 声明一个输出流，将文件写出
        FileOutputStream out = new FileOutputStream(new File(destFile));

        // 读写数据
        IOUtils.copyBytes(inputStream, out, conf);

        // 关闭流
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(out);


    }


}

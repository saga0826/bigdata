package com.atguigu.mr.etl;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * 1.处理缓存文件：将job中设置的缓存路径获取到
 * 2：根据缓存路径再结合输入流把pd.txt内容写入到内存容器中维护
 */
public class EtlJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private Text outk = new Text();


    /**
     * 处理mapjoin逻辑
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split(" ");
        //遍历集合，将长度小于等于11 的过滤掉
        for (String s1 : split) {
           if(s1.length()>11){
               outk.set(s1);
               context.write(outk,NullWritable.get());
           }else{
               return;
           }
        }
    }
}

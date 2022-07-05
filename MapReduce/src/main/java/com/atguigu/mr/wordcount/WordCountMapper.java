package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义的Mapper类，需要集成Hadoop提供的Mapper 并且根据具体业务制定输入数据和输出数据的数据类型
 * 输入数据类型
 * KEYIN 读取文件的偏移量 数字 （LongWritable）
 * VALUEIN 读取文件的一行数据 （Text）
 *
 * 输出数据类型
 * KEYOUT 输出数据的key类型 就是一个单词 (Text)
 * VALUEOUT 输出数据的value类型 给单词标记1数字 （IntWritable）
 */
public class  WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //添加计数器
        context.getCounter("MapTask","setup").increment(1);
//        System.out.println("setup执行了");
    }

    /**
     * Map阶段核心业务处理方法，每输入一行数据会调用一次map方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //添加计数器
        context.getCounter("MapTask","map").increment(1);
        /**
         * 获取当前数据
         */
        String line = value.toString();
        //切割数据
        String[] datas = line.split(" ");
        //遍历集合，封装输出数据 key和value
        for (String data : datas) {
            outk.set(data);
            context.write(outk,outv);
        }
//        System.out.println("map执行了");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //添加计数器
        context.getCounter("MapTask","cleanup").increment(1);
//        System.out.println("cleanup执行了");
    }
}

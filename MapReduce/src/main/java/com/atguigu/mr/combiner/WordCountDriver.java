package com.atguigu.mr.combiner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MR程序的驱动类，主要用于提交MR程序
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //声明配置对象
        Configuration conf = new Configuration();
        //声明job对象
        Job job = Job.getInstance();
        //指定当前job的驱动类
        job.setJarByClass(WordCountDriver.class);
        //指定当前job的Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //指定map端输出的key类型和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定最终（Reducer端）输出的key类型和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定输入数据的目录和输出数据的目录
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //小文件
        //指定CombineTextInputFormat中的切片最大值
//        CombineTextInputFormat.setMaxInputSplitSize(job,4194304*5);
        //制定ReducerTask数量
//        job.setNumReduceTasks(2);
        //制定Inputformat的具体实现
//        job.setInputFormatClass(CombineTextInputFormat.class);
        //指定自定义的Combiner
        job.setCombinerClass(WordCountCombiner.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\temp\\bigdata\\mr\\in\\wcinput"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\temp\\bigdata\\mr\\out\\wcoutput6"));
        //提交job
        job.waitForCompletion(true);
    }
}

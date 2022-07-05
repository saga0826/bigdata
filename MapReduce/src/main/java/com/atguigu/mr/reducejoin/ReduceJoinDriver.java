package com.atguigu.mr.reducejoin;


import com.atguigu.mr.outputformat.LogDriver;
import com.atguigu.mr.outputformat.LogMapper;
import com.atguigu.mr.outputformat.LogOutputFormat;
import com.atguigu.mr.outputformat.LogReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReduceJoinDriver.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Orderpd.class);
        job.setOutputKeyClass(Orderpd.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\temp\\bigdata\\mr\\in\\reducejoin"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\temp\\bigdata\\mr\\out\\reducejoin1"));

        job.waitForCompletion(true);
    }
}

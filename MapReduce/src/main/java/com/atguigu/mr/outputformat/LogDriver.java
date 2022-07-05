package com.atguigu.mr.outputformat;

import com.atguigu.mr.reducejoin.Orderpd;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Orderpd.class);
        job.setOutputValueClass(NullWritable.class);
        //制定自定的outputformat
        job.setOutputFormatClass(LogOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\temp\\bigdata\\mr\\in\\log"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\temp\\bigdata\\mr\\out\\logs\\log1"));
        job.waitForCompletion(true);
    }
}

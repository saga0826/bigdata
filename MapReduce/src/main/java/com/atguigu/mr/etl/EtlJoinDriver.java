package com.atguigu.mr.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class EtlJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(EtlJoinDriver.class);
        job.setMapperClass(EtlJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置Reduce的数量为0
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("E:\\temp\\bigdata\\mr\\in\\ETL"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\temp\\bigdata\\mr\\out\\ETL"));

        job.waitForCompletion(true);
    }
}

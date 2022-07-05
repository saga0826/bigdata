package com.atguigu.mr.mapperjoin;

import com.atguigu.mr.reducejoin.Orderpd;
import com.atguigu.mr.reducejoin.ReduceJoinDriver;
import com.atguigu.mr.reducejoin.ReduceJoinMapper;
import com.atguigu.mr.reducejoin.ReduceJoinReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MapperJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapperJoinDriver.class);
        job.setMapperClass(MapperJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置Reduce的数量为0
        job.setNumReduceTasks(0);


        //设置缓存路径
        job.addCacheFile(URI.create("file:///E:/temp/bigdata/mr/in/cachefile/pd.txt"));
        FileInputFormat.setInputPaths(job, new Path("E:\\temp\\bigdata\\mr\\in\\mapjoin"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\temp\\bigdata\\mr\\out\\mapjoin1"));

        job.waitForCompletion(true);
    }
}

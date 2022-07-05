package com.atguigu.mr.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //制定ReducerTask数量
        job.setNumReduceTasks(5);
        //指定自定义的分区器对象实现
        job.setPartitionerClass(PhonePartitioner.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\temp\\bigdata\\mr\\in\\phone_data"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\temp\\bigdata\\mr\\out\\phone_data_out1"));

        job.waitForCompletion(true);
    }
}

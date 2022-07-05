package com.atguigu.mr.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.rmi.runtime.Log;

import java.io.IOException;

/**
 * 自定义OutputFormat，需要集成Hadoop提供的OutputFormat
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        LogRecordWriter lrw = new LogRecordWriter(job);
        return lrw;
    }
}

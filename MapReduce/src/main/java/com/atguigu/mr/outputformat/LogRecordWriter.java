package com.atguigu.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 自定义LogRecordWriter 对象。需要集成Hadoop提供的RecordWriter
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    //定义输出路径
    private String atguiguPath = "E:\\temp\\bigdata\\mr\\out\\logs\\atguigu.txt";
    private String otherPath = "E:\\temp\\bigdata\\mr\\out\\logs\\other.txt";
    private FileSystem fs;
    private  FSDataOutputStream atGuiguOut;
    private  FSDataOutputStream otherOut;
    public LogRecordWriter(TaskAttemptContext job) throws IOException {
        fs = FileSystem.get(job.getConfiguration());
        atGuiguOut = fs.create(new Path(atguiguPath));
        otherOut = fs.create(new Path(otherPath));
    }

    /**
     * 实现写数据的逻辑
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
          //获取当前输入数据
        String logData = key.toString();
        if(logData.contains("atguigu")){
            atGuiguOut.writeBytes(logData+"\n");
        }else{
            otherOut.writeBytes(logData+"\n");
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atGuiguOut);
        IOUtils.closeStream(otherOut);
    }
}

package com.atguigu.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义的CountCombiner类，需要继承Reducer类
 * TODO 注意 CountCombiner流程发生在map阶段
 */
public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        //遍历values
        for (IntWritable value : values) {
            total+=value.get();
        }
        //封装key和value
        outk.set(key);
        outv.set(total);
        context.write(outk,outv);
    }
}

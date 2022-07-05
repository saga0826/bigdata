package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义的Reducer类，需要集成Hadoop提供的Reducer 并且根据具体业务制定输入数据和输出数据的数据类型
 * 输入数据类型
 * KEYIN map端输出的key的数据类型 （Text）
 * VALUEIN map端输出的value的数据类型  （IntWritable）
 *
 * 输出数据类型
 * KEYOUT 输出数据的key类型 就是一个单词 (Text)
 * VALUEOUT 输出数据的value类型 单词出现的总次数 （IntWritable）
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private Text outk = new Text();
    private IntWritable outv = new IntWritable();
    /**
     * Reduce阶段核心业务逻辑处理方法，一组相同key的values会调用一次reduce方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
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

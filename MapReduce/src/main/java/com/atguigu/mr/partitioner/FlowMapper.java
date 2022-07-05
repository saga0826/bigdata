package com.atguigu.mr.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    private Text outk = new Text();
    private FlowBean outv = new FlowBean();
    /**
     * 核心业务逻辑处理
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取当前行数据
        String line = value.toString();
        //切割数据  1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String[] phoneDatas = line.split("\t");
        //获取输出数据的key（手机号）
        outk.set(phoneDatas[1]);
        //获取输出数据的value
        outv.setUpFlow(Integer.parseInt(phoneDatas[phoneDatas.length-3]));
        outv.setDownFlow(Integer.parseInt(phoneDatas[phoneDatas.length-2]));
        outv.setSumFlow();

        //将数据输出
        context.write(outk,outv);
    }
}

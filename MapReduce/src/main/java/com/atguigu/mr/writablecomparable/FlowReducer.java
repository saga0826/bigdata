package com.atguigu.mr.writablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean,Text,Text, FlowBean> {
    private FlowBean outv = new FlowBean();
    /**
     * 核心业务逻辑处理
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历输出
        for (Text value : values) {
            context.write(value,key);
        }
    }
}

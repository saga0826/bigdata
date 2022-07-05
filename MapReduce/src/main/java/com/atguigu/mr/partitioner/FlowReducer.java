package com.atguigu.mr.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
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
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 遍历当前一组相同key的values
        int totalUpFlow = 0;
        int totalDownFlow = 0;
//        int totalSumFlow = 0;
        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
//            totalSumFlow += value.getSumFlow();
        }

        // 封装输出数据的value
        outv.setUpFlow(totalUpFlow);
        outv.setDownFlow(totalDownFlow);
//        outv.setSumFlow(totalSumFlow);
        outv.setSumFlow();
        // 结果写出
        context.write(key, outv);
    }
}

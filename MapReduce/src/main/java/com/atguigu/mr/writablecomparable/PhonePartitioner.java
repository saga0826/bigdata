package com.atguigu.mr.writablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器对象，需要继承Hadoop提供的Partitioner对象
 */
public class PhonePartitioner extends Partitioner<FlowBean,Text> {

    /**
     * 定义当前kv所属分区的规则
     * @param text
     * @param flowBean
     * @param i
     * @return
     */
    public int getPartition(FlowBean flowBean,Text text,  int i) {
        int phonePartitioners;
        String phoneNum = text.toString();
        if(phoneNum.startsWith("136")){
            phonePartitioners = 0;
        }else if(phoneNum.startsWith("137")){
            phonePartitioners = 1;
        }else if(phoneNum.startsWith("138")){
            phonePartitioners = 2;
        } else if(phoneNum.startsWith("139")){
            phonePartitioners = 3;
        }else{
            phonePartitioners = 4;
        }
        return phonePartitioners;
    }
}

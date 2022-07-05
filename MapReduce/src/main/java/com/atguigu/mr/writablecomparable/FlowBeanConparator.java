package com.atguigu.mr.writablecomparable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义比较器对象，需要集成Hadoop提供的比较器对象WritableComparator
 */
public class FlowBeanConparator extends WritableComparator {

    //指定当前比较器对象为谁使用
    public FlowBeanConparator() {
        super(FlowBean.class,true);
    }

    /**
     * FlowBean 中总流量进行升序
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowBean abean = (FlowBean) a;
        FlowBean bbean = (FlowBean) b;
        return abean.getSumFlow().compareTo(bbean.getSumFlow());
    }
}

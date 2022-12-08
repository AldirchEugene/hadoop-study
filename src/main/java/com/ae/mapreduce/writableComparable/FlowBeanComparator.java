package com.ae.mapreduce.writableComparable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义比较器，继承hadoop提供的 WritableComparator类
 */
public class FlowBeanComparator extends WritableComparator {

    /**
     * 必须调用父类的构造器初始化需要比较的对象
     */
    public FlowBeanComparator() {
        super(FlowBean.class,true);
    }

    /**
     * 实现compare方法自定义比较规则:升序
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowBean a1 = (FlowBean) a;
        FlowBean b1 = (FlowBean) b;
        return a1.getSumFlow() - b1.getSumFlow();
    }
}

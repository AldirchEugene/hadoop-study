package com.ae.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义一个分区器，需要继承Hadoop提供的Partitioner接口
 */
public class PhonePartitioner extends Partitioner<Text, FlowBean> {

    /**
     * 定义当前kv所属分区的规则
     * 电话号码以
     * 136 --> 0
     * 137 --> 1
     * 138 --> 2
     * 139 --> 3
     * 其它 --> 4
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        int partitionNum;
        String phoneNum = text.toString();

        if(phoneNum.startsWith("136")){
            partitionNum = 0;
        }else if(phoneNum.startsWith("137")){
            partitionNum = 1;
        }else if(phoneNum.startsWith("138")){
            partitionNum = 2;
        }else if(phoneNum.startsWith("139")){
            partitionNum = 3;
        }else {
            partitionNum = 4;
        }

        return partitionNum;
    }
}

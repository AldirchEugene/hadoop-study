package com.ae.mapreduce.serialization;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FlowReducer  extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 循环遍历累加求和
        int totalUpFlow = 0;
        int totalDownFlow = 0;
        int totalSumFlow = 0;
        for (FlowBean flowBean : values) {
            totalUpFlow += flowBean.getUpFlow();
            totalDownFlow += flowBean.getDownFlow();
            totalSumFlow += flowBean.getSumFlow();
        }

        // 封装输出的value
        outV.setUpFlow(totalUpFlow);
        outV.setDownFlow(totalDownFlow);
        outV.setSumFlow(totalSumFlow);

        // 写出
        context.write(key, outV);
    }
}

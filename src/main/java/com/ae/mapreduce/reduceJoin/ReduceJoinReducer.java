package com.ae.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoinReducer extends Reducer<Text, Orderpd,Orderpd, NullWritable> {

   private ArrayList<Orderpd> orderList = new ArrayList<Orderpd>();
   private Orderpd pd = new Orderpd();

    /**
     * 核心处理方法--> 接收Map阶段整合好的数据 进行最终的join操作
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Orderpd> values, Context context) throws IOException, InterruptedException {
        // 遍历当前相同key的一组values
        for (Orderpd orderpd : values) {
            // 判断当前的数据来源
            if(orderpd.getTitle().equals("order")){
                try {
                    // 当前数据来源order.txt 文件,将当前数据管理到一个集合当中
                    Orderpd thisOrderpd = new Orderpd();
                    // 将当前传入Orderpd 复制到 thisOrderpd
                    BeanUtils.copyProperties(thisOrderpd, orderpd);
                    orderList.add(thisOrderpd);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else {
                // 当前数据来源pd.txt 文件
                // 将当前传入Orderpd 复制到 thisOrderpd
                try {
                    BeanUtils.copyProperties(pd, orderpd);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        // 进行Join操作
        for (Orderpd op : orderList) {
            op.setPname(pd.getPname());
            // 将数据写出
            context.write(op, NullWritable.get());
        }
        // 清空orderList
        orderList.clear();

    }
}

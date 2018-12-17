package com.sanmu.cloud.wc1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2018-02-24 16:27
 **/
public class WCReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

    protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        Iterator iterator = values.iterator();
        int sum = 0;

        for(IntWritable value : values){
            sum++;
        }

        context.write(key,new IntWritable(sum));
    }
}

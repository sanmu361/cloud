package com.sanmu.cloud.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sanmu on 2016/12/18.
 */
public class WCReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
    protected void reduce(Text key, Iterable<IntWritable> intWritables, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable i : intWritables){
            sum += i.get();
        }
        context.write(key,new IntWritable(sum));
    }
}

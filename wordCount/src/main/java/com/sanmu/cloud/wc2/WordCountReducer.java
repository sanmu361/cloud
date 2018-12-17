package com.sanmu.cloud.wc2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2018-11-06 15:53
 **/
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    protected void reudce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            sum += iterator.next().get();
        }

        context.write(key,new IntWritable(sum));
    }
}

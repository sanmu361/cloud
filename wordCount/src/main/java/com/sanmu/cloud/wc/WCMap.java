package com.sanmu.cloud.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by sanmu on 2016/12/18.
 */
public class WCMap extends Mapper<LongWritable,Text,Text,IntWritable> {
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        String[] strs = StringUtils.split(value.toString(),' ');

        for(String s : strs){
            context.write(new Text(s),new IntWritable(1));
        }
    }
}

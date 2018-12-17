package com.sanmu.cloud.wc1;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.List;

/**
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2018-02-24 16:20
 **/
public class WCMap extends Mapper<LongWritable, Text, Text, IntWritable> {


    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String sentence = value.toString();

        List<String> list = Splitter.on(" ").trimResults().omitEmptyStrings().splitToList(sentence);

        for(String keyOut : list){
            context.write(new Text(keyOut),new IntWritable(1));
        }

    }
}

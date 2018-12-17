package com.sanmu.cloud.wc2;

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
 * @create 2018-11-06 15:53
 **/
public class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable>{


    protected void map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException {

        String secentce = value.toString();

        List<String> words = Splitter.on(" ").omitEmptyStrings().trimResults().splitToList(secentce);

        for(String word : words){
            context.write(new Text(word),new IntWritable(1));
        }

    }
}

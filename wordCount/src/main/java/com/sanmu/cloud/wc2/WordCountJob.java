package com.sanmu.cloud.wc2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2018-11-06 15:53
 **/
public class WordCountJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"wordCount");

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(WordCountReducer.class);
//        job.setSortComparatorClass();
//        job.setGroupingComparatorClass();
        job.setReducerClass(WordCountReducer.class);

        FileInputFormat.addInputPath(job,new Path(""));
        FileOutputFormat.setOutputPath(job,new Path(""));

        job.waitForCompletion(true);
        job.submit();
    }
}

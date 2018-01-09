package com.sanmu.cloud.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class WeatherJob {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        conf.set("mapred.jar","E:\\IDEA\\WordCount\\out\\artifacts\\WordCount_jar\\WordCount.jar");

        Job job = Job.getInstance(conf);

        job .setJarByClass(WeatherJob.class);

        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(Temperature.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setReducerClass(TemperatureReducer.class);

        job.setPartitionerClass();

    }
}

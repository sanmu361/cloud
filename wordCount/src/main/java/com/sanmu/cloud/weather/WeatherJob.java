package com.sanmu.cloud.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;

public class WeatherJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("mapred.jar","E:\\IDEA\\WordCount\\out\\artifacts\\WordCount_jar\\WordCount.jar");

        Job job = Job.getInstance(conf);

        job .setJarByClass(WeatherJob.class);

        job.setMapperClass(TemperatureMapper.class);
        job.setMapOutputKeyClass(Temperature.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setReducerClass(TemperatureReducer.class);

        job.setPartitionerClass(TemperaturePartition.class);
        job.setSortComparatorClass(TemperatureSort.class);

        job.setGroupingComparatorClass(TemperatureGroup.class);

        job.setNumReduceTasks(3);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job,new Path("/job"));


        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(new Path("out"))){
            fs.delete(new Path(""),true);
        }
        FileOutputFormat.setOutputPath(job,new Path(""));

        boolean f = job.waitForCompletion(true);

        if(f){
            System.out.println("job Success");
        }
    }
}

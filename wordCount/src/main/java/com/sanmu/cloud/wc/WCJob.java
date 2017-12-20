package com.sanmu.cloud.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by sanmu on 2016/12/18.
 */
public class WCJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("mapred.jar","E:\\IDEA\\WordCount\\out\\artifacts\\WordCount_jar\\WordCount.jar");

        Job job = Job.getInstance(conf);

        job.setJarByClass(WCJob.class);
        job.setMapperClass(WCMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WCReduce.class);
        job.setCombinerClass(WCReduce.class);

        FileInputFormat.addInputPath(job,new Path("/sanmu/data/input/wc"));

        Path outPath = new Path("/sanmu/data/output");

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }

        FileOutputFormat.setOutputPath(job,outPath);
        boolean flag = job.waitForCompletion(true);
        if(flag){
            System.out.println("job success !");
        }

    }
}

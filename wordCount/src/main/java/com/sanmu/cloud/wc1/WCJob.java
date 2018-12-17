package com.sanmu.cloud.wc1;

import com.sanmu.cloud.wc.WCMap;
import com.sanmu.cloud.wc.WCReduce;
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
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2018-02-24 16:02
 **/
public class WCJob {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJar("WCJOB");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(WCMap.class);


        job.setReducerClass(WCReduce.class);

        FileInputFormat.addInputPath(job,new Path(""));

        FileOutputFormat.setOutputPath(job,new Path(""));

        FileSystem fileSystem = FileSystem.get(conf);

        if(fileSystem.exists(new Path(""))){
            fileSystem.delete(new Path(""),false);
        }


    }
}

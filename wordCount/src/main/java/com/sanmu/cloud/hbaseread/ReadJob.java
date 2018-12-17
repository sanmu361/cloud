package com.sanmu.cloud.hbaseread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import java.io.IOException;
import java.util.HashMap;

/**
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2018-12-17 19:21
 **/
public class ReadJob {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        Scan scan = new Scan();
        scan.addColumn("info".getBytes(),"age".getBytes());

        TableMapReduceUtil.initTableMapperJob("table_name",scan, HbaseMapper.class,LongWritable.class,Text.class,job);


        job.setJar("HbeseRead");
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapperClass(HbaseMapper.class);
        job.setReducerClass(HbaseReducer.class);

        FileOutputFormat.setOutputPath(job,new Path(""));

    }
}

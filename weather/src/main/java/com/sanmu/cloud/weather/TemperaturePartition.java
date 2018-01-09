package com.sanmu.cloud.weather;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class TemperaturePartition extends HashPartitioner<Temperature,IntWritable> {

    public int getPARTITION(Temperature key,IntWritable value,int numReduceTasks){
        return (key.getYear() - 1949) % numReduceTasks;
    }
}

package com.sanmu.cloud.weather;

import com.mysql.jdbc.util.Base64Decoder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TemperatureReducer extends Reducer<Temperature,IntWritable,Text,NullWritable> {


    protected void reduce(Temperature key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int flag = 0;

        for(IntWritable i : values){
            if(flag >= 2){
                break;
            }

            String msg = key.getYear() + "-" + key.getMouth() + "-" + i.get();

            context.write(new Text(msg),NullWritable.get());
        }

    }

}

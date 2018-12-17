package com.sanmu.cloud.weather;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

public class TemperatureMapper extends Mapper<LongWritable,Text,Temperature,IntWritable> {
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

      List<String> list = Splitter.on("\t").omitEmptyStrings().trimResults().splitToList(value.toString());

      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      Calendar calendar = Calendar.getInstance();

        try {
            calendar.setTime(dateFormat.parse(list.get(0)));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int wd = Integer.parseInt(list.get(1).substring(0,list.get(1).indexOf("c")));

        Temperature temperature = new Temperature();
        temperature.setYear(calendar.get(Calendar.YEAR));
        temperature.setMouth(calendar.get(calendar.MONTH));
        temperature.setWd(wd);

        context.write(temperature,new IntWritable(wd));
    }
}

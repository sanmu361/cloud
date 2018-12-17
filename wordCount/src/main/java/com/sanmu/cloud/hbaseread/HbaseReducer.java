package com.sanmu.cloud.hbaseread;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2018-12-17 19:41
 **/
public class HbaseReducer extends TableReducer <Text, NullWritable, NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values,Context context)
            throws IOException, InterruptedException {

        String[] split = key.toString().split(",");

        Put put = new Put(split[0].getBytes());

        put.addColumn("info".getBytes(), "name".getBytes(), split[1].getBytes());
        put.addColumn("info".getBytes(), "sex".getBytes(), split[2].getBytes());
        put.addColumn("info".getBytes(), "age".getBytes(), split[3].getBytes());
        put.addColumn("info".getBytes(), "department".getBytes(), split[4].getBytes());

        context.write(NullWritable.get(), put);

    }
}

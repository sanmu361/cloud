package com.sanmu.cloud.weather1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2018-02-24 17:06
 **/
public class Temperature implements WritableComparable<Temperature> {

    private int year;
    private int month;

    public double wd;

    public void setYear(int year) {
        this.year = year;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void setWd(double wd) {
        this.wd = wd;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public double getWd() {
        return wd;
    }

    @Override
    public int compareTo(Temperature o) {
        int r1 = Integer.compare(this.year,o.getYear());
        if(r1 == 0){
            int r2 = Integer.compare(this.month,this.month);
        }
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}

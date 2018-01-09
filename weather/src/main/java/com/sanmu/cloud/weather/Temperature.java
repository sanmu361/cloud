package com.sanmu.cloud.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Temperature implements WritableComparable {
    private int year;
    private int mouth;
    private int wd;

    public void setYear(int year) {
        this.year = year;
    }

    public void setMouth(int mouth) {
        this.mouth = mouth;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    public int getYear() {
        return year;
    }

    public int getMouth() {
        return mouth;
    }

    public int getWd() {
        return wd;
    }

    @Override
    public int compareTo(Object O){

        Temperature temperature = (Temperature)O;

        int c1 = Integer.compare(this.year,temperature.getYear());

        if(c1 == 0){
            int c2 = Integer.compare(this.mouth, temperature.getMouth());
            if(c2 == 0){
                return Integer.compare(this.wd,temperature.wd);
            }
            return c2;
        }
        return c1;

    }


    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}

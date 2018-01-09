package com.sanmu.cloud.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TemperatureSort extends WritableComparator {

    public TemperatureSort(){
        super(Temperature.class,true);
    }

    public int compar(WritableComparable a, WritableComparable b){
        Temperature t1 = (Temperature)a;
        Temperature t2 = (Temperature)b;

        if(){

        }
    }
}

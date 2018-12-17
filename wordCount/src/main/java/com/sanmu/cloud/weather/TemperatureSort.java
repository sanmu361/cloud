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

        int c1 = Integer.compare(t1.getYear(),t2.getYear());

        if(c1 == 0){
            int c2 = Integer.compare(t1.getMouth(), t2.getMouth());
            if(c2 == 0){
                return 0 - Integer.compare(t1.getWd(),t2.getWd());
            }
            return c2;
        }
        return c1;
    }
}

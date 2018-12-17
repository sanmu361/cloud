package com.sanmu.cloud.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * ${DESCRIPTION}
 *
 * @author yansen
 * @create 2018-01-09 12:11
 **/
public class TemperatureGroup extends WritableComparator {

    public TemperatureGroup(){
        super(Temperature.class,true);
    }

    public int compare(WritableComparable a, WritableComparable b){
        Temperature k1 = (Temperature) a;
        Temperature k2 = (Temperature) b;

        int r1 = Integer.compare(k1.getYear(),k2.getYear());

        if(r1 == 0){
            return Integer.compare(k1.getMouth(),k2.getMouth());
        }else{
            return r1;
        }

    }
}

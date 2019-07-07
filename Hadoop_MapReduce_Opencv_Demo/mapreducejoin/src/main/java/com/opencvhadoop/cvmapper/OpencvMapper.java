package com.opencvhadoop.cvmapper;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OpencvMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }
}

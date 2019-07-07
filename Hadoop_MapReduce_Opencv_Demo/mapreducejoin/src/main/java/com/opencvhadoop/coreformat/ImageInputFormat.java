package com.opencvhadoop.coreformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public abstract class ImageInputFormat<K,V> extends FileInputFormat<K,V> {

    //image is not split
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}

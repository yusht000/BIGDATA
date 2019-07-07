package com.opencvhadoop.coreformat;

import com.opencvhadoop.corerecord.BufferedImageRecordReader;
import com.opencvhadoop.corewritable.ImageWriteable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class BufferedImageInputFormat extends ImageInputFormat<NullWritable,ImageWriteable> {

    @Override
    public RecordReader<NullWritable, ImageWriteable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new BufferedImageRecordReader();
    }
}

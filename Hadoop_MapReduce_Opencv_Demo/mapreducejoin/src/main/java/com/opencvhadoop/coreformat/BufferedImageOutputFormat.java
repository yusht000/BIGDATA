package com.opencvhadoop.coreformat;

import com.opencvhadoop.corerecord.BufferedImageRecordWriter;
import com.opencvhadoop.corewritable.BufferedImageWriteable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BufferedImageOutputFormat extends FileOutputFormat<NullWritable,BufferedImageWriteable> {
    @Override
    public RecordWriter<NullWritable, BufferedImageWriteable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new BufferedImageRecordWriter(job);
    }
}

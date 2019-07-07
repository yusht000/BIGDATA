package com.opencvhadoop.cvformat;

import com.opencvhadoop.cvrecord.MatImageRecordWriter;
import com.opencvhadoop.cvwritable.MatImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MatImageOutputFormat extends FileOutputFormat<NullWritable,MatImageWritable> {

    @Override
    public RecordWriter<NullWritable, MatImageWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        System.out.println("=====********========");
        return new MatImageRecordWriter(job);
    }
}

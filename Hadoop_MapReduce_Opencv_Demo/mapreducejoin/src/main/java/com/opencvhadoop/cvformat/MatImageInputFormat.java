package com.opencvhadoop.cvformat;

import com.opencvhadoop.coreformat.ImageInputFormat;
import com.opencvhadoop.cvrecord.MatImageRecordReader;
import com.opencvhadoop.cvwritable.MatImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MatImageInputFormat extends ImageInputFormat<NullWritable,MatImageWritable> {

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MatImageRecordReader();
    }
}

package com.opencvhadoop.cvrecord;

import com.opencvhadoop.corerecord.ImageRecordWriter;
import com.opencvhadoop.cvwritable.MatImageWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;

import java.io.IOException;

public class MatImageRecordWriter extends ImageRecordWriter<MatImageWritable> {


    public MatImageRecordWriter(TaskAttemptContext taskAttemptContext) {
        super(taskAttemptContext);
    }

    @Override
    protected void writeImage(MatImageWritable img, FSDataOutputStream imageFileStream) throws IOException {

        MatOfByte mob = new MatOfByte();
        boolean imencode = Imgcodecs.imencode("." + img.getFormat(), img.getImg(), mob);
        imageFileStream.write(mob.toArray());
    }
}

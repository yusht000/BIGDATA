package com.opencvhadoop.corerecord;

import com.opencvhadoop.corewritable.BufferedImageWriteable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import javax.imageio.ImageIO;
import java.io.IOException;


public class BufferedImageRecordWriter extends ImageRecordWriter<BufferedImageWriteable> {


    public BufferedImageRecordWriter(TaskAttemptContext taskAttemptContext) {
        super(taskAttemptContext);
    }
    @Override
    protected void writeImage(BufferedImageWriteable img, FSDataOutputStream imageFileStream) throws IOException {
        ImageIO.write(img.getImg(),img.getFormat(), imageFileStream);
    }


}

package com.opencvhadoop.corerecord;

import com.opencvhadoop.corewritable.BufferedImageWriteable;
import com.opencvhadoop.corewritable.ImageWriteable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;

public class BufferedImageRecordReader extends ImageRecodReader<ImageWriteable> {

    @Override
    protected ImageWriteable readImage(FSDataInputStream fsDataInputStream) throws IOException {

        BufferedImageWriteable biw = new BufferedImageWriteable();
        BufferedImage bi;
        try {
            bi = ImageIO.read(fsDataInputStream);
        } catch (IOException e) {
            bi = null;
        }
        biw.setImg(bi);
        return biw;
    }
}

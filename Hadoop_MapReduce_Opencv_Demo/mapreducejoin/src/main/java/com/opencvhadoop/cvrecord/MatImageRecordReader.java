package com.opencvhadoop.cvrecord;


import com.opencvhadoop.corerecord.ImageRecodReader;
import com.opencvhadoop.cvwritable.MatImageWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MatImageRecordReader extends ImageRecodReader<MatImageWritable> {


    @Override
    protected MatImageWritable readImage(FSDataInputStream fsDataInputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead ;
        byte[] data  = new byte[16384];

        while ((nRead = fsDataInputStream.read(data,0,data.length))!= -1){
            buffer.write(data,0,nRead);
        }
        buffer.flush();
        byte[] tempImageInMemory = buffer.toByteArray();
        buffer.close();

        Mat out = Imgcodecs.imdecode(new MatOfByte(tempImageInMemory), Imgcodecs.IMREAD_ANYCOLOR);

        return  new MatImageWritable(out);
    }
}

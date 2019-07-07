package com.opencvhadoop.cvwritable;

import com.opencvhadoop.corewritable.ImageWriteable;
//import org.bytedeco.javacpp.opencv_core.Mat;
import org.opencv.core.Mat;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatImageWritable extends ImageWriteable<Mat> {



    public MatImageWritable() {
        this.img = new Mat();
        this.format = "undef";
        this.fileName = "unname";

    }

    public  MatImageWritable(Mat mat){
        this();
        this.img = mat;
    }

    public MatImageWritable(Mat mat, String format, String fileName){
        this(mat);
        setFileName(fileName);
        setFormat(format);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        byte[] byteArray = new byte[(int)(img.total() * img.channels())];
        img.get(0,0,byteArray);

        out.writeInt((int)(img.total()*img.channels()));

        out.writeInt(img.width());
        out.writeInt(img.height());
        out.writeInt(img.type());

        out.write(byteArray);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int arraySize = in.readInt();

        int mWidth  = in.readInt();

        int mHeigth = in.readInt();

        int type = in.readInt();

        byte[] bytes = new byte[arraySize];
        in.readFully(bytes);

        this.img = new Mat(mHeigth,mWidth,type);

        this.img.put(0,0,bytes);
    }
}

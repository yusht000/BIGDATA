package com.opencvhadoop.cvexer;

import com.opencvhadoop.cvmapper.OpencvMapper;
import com.opencvhadoop.cvwritable.MatImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.opencv.core.Mat;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.xfeatures2d.SURF;


import java.io.IOException;


public class ImgFeatureDetectorMapper extends OpencvMapper<NullWritable,MatImageWritable,NullWritable,MatImageWritable> {

    private double hessianThreshold = 400;
    private int nOctaves = 4;
    private int nOctaveLayers = 3;
    private boolean extended = false;
    private boolean upright = false;
    private SURF surf = SURF.create(hessianThreshold, nOctaves, nOctaveLayers, extended, upright);
    private Mat src;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

    }

    @Override
    protected void map(NullWritable key, MatImageWritable value, Context context) throws IOException, InterruptedException {

        MatOfKeyPoint matOfKeyPoint = new MatOfKeyPoint();
        surf.detect(value.getImg(),matOfKeyPoint);
        System.out.println(matOfKeyPoint.toArray());

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}

package com.opencvhadoop.corexer;

import com.opencvhadoop.corewritable.BufferedImageWriteable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;

public class Imag2GrayMapper extends Mapper<NullWritable,BufferedImageWriteable,NullWritable,BufferedImageWriteable> {

    private Graphics g;
    private BufferedImage grayImage;
    private BufferedImageWriteable biw = new BufferedImageWriteable();

    @Override
    protected void map(NullWritable key, BufferedImageWriteable value, Context context) throws IOException, InterruptedException {
        BufferedImage colorImage = value.getImg();

        if (colorImage != null) {

            grayImage = new BufferedImage(colorImage.getWidth(),colorImage.getHeight(),BufferedImage.TYPE_BYTE_GRAY);

            g=grayImage.getGraphics();
            g.drawImage(colorImage,0,0,null);
            g.dispose();
            biw.setImg(grayImage);
            biw.setFileName(value.getFileName());
            biw.setFormat(value.getFormat());
            context.write(NullWritable.get(),biw);
        }
    }
}

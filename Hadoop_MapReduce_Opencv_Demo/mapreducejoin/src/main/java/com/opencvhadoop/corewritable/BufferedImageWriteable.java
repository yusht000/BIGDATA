package com.opencvhadoop.corewritable;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

public class BufferedImageWriteable extends ImageWriteable<BufferedImage> {

    public BufferedImageWriteable() {
    }

    public BufferedImageWriteable(BufferedImage _img) {
        this();
        img = _img;
    }

    public BufferedImageWriteable(String fileName, String format, BufferedImage _img) {
        this(_img);
        this.fileName = fileName;
        this.format = format;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ImageIO.write(img, this.format, baos);
        baos.flush();
        byte[] bytes = baos.toByteArray();
        baos.close();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int bArraySize = in.readInt();
        byte[] bytes = new byte[bArraySize];
        in.readFully(bytes);
        img = ImageIO.read(new ByteArrayInputStream(bytes));
    }
}

package com.opencvhadoop.corewritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class ImageWriteable<I> implements Writable {


    protected I img;
    protected String fileName;
    protected String format;


    public I getImg() {
        return img;
    }

    public void setImg(I img) {
        this.img = img;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        Text.writeString(out,fileName);

        Text.writeString(out,format);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.fileName = Text.readString(in);

        this.format   = Text.readString(in);
    }
}

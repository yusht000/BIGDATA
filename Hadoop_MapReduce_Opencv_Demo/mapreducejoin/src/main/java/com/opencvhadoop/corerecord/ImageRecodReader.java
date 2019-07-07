package com.opencvhadoop.corerecord;

import com.opencvhadoop.corewritable.ImageWriteable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public abstract class ImageRecodReader<I extends ImageWriteable> extends RecordReader<NullWritable, I> {


    protected String fileName;
    protected FSDataInputStream fileStream;
    private boolean proccessed = false;

    protected I img;

    protected abstract I readImage(FSDataInputStream fsDataInputStream) throws IOException;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) split;
        final Path path = fileSplit.getPath();
        fileName = path.getName();

        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        fileStream = fileSystem.open(path);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!proccessed) {

            img = readImage(fileStream);
            if (img != null) {
                setImageFileNameAndFormat();
                proccessed = true;
                return true;
            }
        }
        return false;

    }

    private void setImageFileNameAndFormat() {

        if ((fileName != null) && (img != null)) {
            int dotPos = fileName.lastIndexOf(".");
            if (dotPos > -1) {
                img.setFileName(fileName.substring(0, dotPos));
                img.setFormat(fileName.substring(dotPos + 1));
            } else {
                img.setFileName(fileName);
            }

        }
    }


    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public I getCurrentValue() throws IOException, InterruptedException {
        return img;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return proccessed ? 1 : 0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(fileStream);
    }
}

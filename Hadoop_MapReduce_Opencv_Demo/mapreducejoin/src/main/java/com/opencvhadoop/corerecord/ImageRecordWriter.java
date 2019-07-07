package com.opencvhadoop.corerecord;

import com.opencvhadoop.corewritable.ImageWriteable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public abstract class ImageRecordWriter<I extends ImageWriteable> extends RecordWriter<NullWritable, I> {

    protected final TaskAttemptContext taskAttemptContext;

    public ImageRecordWriter(TaskAttemptContext taskAttemptContext) {
        this.taskAttemptContext = taskAttemptContext;
    }

    protected abstract void writeImage(I img, FSDataOutputStream imageFileStream)throws IOException;

    @Override
    public void write(NullWritable key, I img) throws IOException, InterruptedException {
        if (img.getImg() != null) {

            FSDataOutputStream imageFile = null;
            Configuration configuration = taskAttemptContext.getConfiguration();
            Path file = FileOutputFormat.getOutputPath(taskAttemptContext);

            FileSystem fileSystem = file.getFileSystem(configuration);
            Path imageFilePath = new Path(file, img.getFileName() + "." + img.getFormat());

            try {
                imageFile = fileSystem.create(imageFilePath);
                writeImage(img, imageFile);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(imageFile);
            }
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

    }
}

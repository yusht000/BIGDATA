package com.opencvhadoop.corexer;

import com.opencvhadoop.coreformat.BufferedImageInputFormat;
import com.opencvhadoop.coreformat.BufferedImageOutputFormat;
import com.opencvhadoop.corewritable.BufferedImageWriteable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class Imag2GrayDriver extends Configured implements Tool {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Imag2GrayDriver.class);

        job.setMapperClass(Imag2GrayMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(BufferedImageInputFormat.class);
        job.setOutputFormatClass(BufferedImageOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BufferedImageWriteable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BufferedImageWriteable.class);

        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\Echo\\Desktop\\image"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\Echo\\Desktop\\outImage"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

    @Override
    public int run(String[] args) throws Exception {

     return 0;
    }
}

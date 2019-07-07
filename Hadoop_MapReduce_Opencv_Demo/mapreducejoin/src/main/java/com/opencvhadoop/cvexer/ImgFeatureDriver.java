package com.opencvhadoop.cvexer;

import com.opencvhadoop.cvformat.MatImageInputFormat;
import com.opencvhadoop.cvformat.MatImageOutputFormat;
import com.opencvhadoop.cvwritable.MatImageWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ImgFeatureDriver extends Configured implements Tool {


    static {
        System.load("D:\\BigData\\JavaOpencv\\opencv_java320.dll");
   }

    public static void main(String[] args) throws Exception {

        args = new String[]{"C:\\Users\\Echo\\Desktop\\image", "C:\\Users\\Echo\\Desktop\\outImage"};
        int res = ToolRunner.run(new ImgFeatureDriver(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(ImgFeatureDriver.class);
        job.setMapperClass(ImgFeatureDetectorMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MatImageWritable.class);

        job.setInputFormatClass(MatImageInputFormat.class);
        job.setOutputFormatClass(MatImageOutputFormat.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MatImageWritable.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
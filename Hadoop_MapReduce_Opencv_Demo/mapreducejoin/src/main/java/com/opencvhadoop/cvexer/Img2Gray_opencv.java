package com.opencvhadoop.cvexer;

import com.opencvhadoop.cvformat.MatImageInputFormat;
import com.opencvhadoop.cvformat.MatImageOutputFormat;
import com.opencvhadoop.cvmapper.OpencvMapper;
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
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;

import java.io.IOException;

public class Img2Gray_opencv extends Configured implements Tool {

    static {
        System.load("D:\\BigData\\JavaOpencv\\opencv_java320.dll");
    }

    public static void main(String[] args) throws Exception {

        args = new String[]{"C:\\Users\\Echo\\Desktop\\image","C:\\Users\\Echo\\Desktop\\outImage"};
        int res = ToolRunner.run(new Img2Gray_opencv(),args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Img2Gray_opencv.class);
        job.setMapperClass(Img2GrayOpencMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MatImageWritable.class);

        job.setInputFormatClass(MatImageInputFormat.class);
        job.setOutputFormatClass(MatImageOutputFormat.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MatImageWritable.class);

        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class Img2GrayOpencMapper extends OpencvMapper<NullWritable, MatImageWritable, NullWritable, MatImageWritable> {

        protected MatImageWritable outValue = new MatImageWritable();
        @Override
        protected void map(NullWritable key, MatImageWritable value, Context context) throws IOException, InterruptedException {
            Mat image = value.getImg();
            Mat result = new Mat(image.height(), image.width(), CvType.CV_8UC3);

            if (image.type() == CvType.CV_8UC3) {
                Imgproc.cvtColor(image, result, Imgproc.COLOR_RGB2GRAY);
            } else {
                result = image;
            }
            outValue.setFormat(value.getFormat());
            outValue.setFileName(value.getFileName());
            outValue.setImg(result);
            context.write(NullWritable.get(),outValue);
        }
    }


}

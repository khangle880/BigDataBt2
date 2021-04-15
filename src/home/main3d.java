package home;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import components.classes.*;
import components.job.job3d.*;

public class main3d {
    public static String OUTPUT_FILE_NAME = "/part-r-00000";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BaiTap3d");
        job.setJarByClass(main3d.class);
        job.setMapperClass(Job3dMapper.class);
        job.setReducerClass(Job3dReducer.class);
        job.setOutputKeyClass(Couple.class);
        job.setOutputValueClass(Couple.class);
        FileInputFormat.addInputPath(job, new Path(args[0] + OUTPUT_FILE_NAME));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

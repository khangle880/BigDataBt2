package home;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import components.classes.*;
import components.job.job3b.*;

public class main3b {
    public static String OUTPUT_FILE_NAME = "/part-r-00000";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "BaiTap3b");
        job1.setJarByClass(main3b.class);
        job1.setMapperClass(Job3bMapper.class);
        job1.setReducerClass(Job3bReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Couple.class);
        FileInputFormat.addInputPath(job1, new Path(args[0] + OUTPUT_FILE_NAME));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}

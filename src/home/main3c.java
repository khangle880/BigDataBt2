package home;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import components.classes.*;
import components.job.job3c.*;

public class main3c {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BaiTap3c");
        job.setJarByClass(main3c.class);
        job.setMapperClass(Job3cMapper.class);
        job.setCombinerClass(Job3cReducer.class);
        job.setReducerClass(Job3cReducer.class);
        job.setOutputKeyClass(Triple.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

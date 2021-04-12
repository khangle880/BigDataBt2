package home;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import components.classes.*;
import components.job.job3b.*;

public class main3b {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BaiTap3b");
        job.setJarByClass(main3b.class);
        job.setMapperClass(Job3bMapper.class);
        // job.setCombinerClass(Job3bReducer.class);
        job.setReducerClass(Job3bReducer.class);
        job.setOutputKeyClass(Couple.class);
        job.setOutputValueClass(Couple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

package components.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import components.classes.Couple;

public class job3b {
    public static class Job3bMapper extends Mapper<Object, Text, IntWritable, Couple> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] arrLines = value.toString().split("\n");

            for (String sLine : arrLines) {
                sLine = sLine.replace("(", "");
                sLine = sLine.replaceAll(" :", "");
                sLine = sLine.replace(")\t", " ");

                String[] arrItems = sLine.split(" ");

                IntWritable a = new IntWritable(Integer.parseInt(arrItems[0]));
                IntWritable b = new IntWritable(Integer.parseInt(arrItems[1]));
                IntWritable v = new IntWritable(Integer.parseInt(arrItems[2]));
                context.write(a, new Couple(b, v));
                context.write(b, new Couple(a, v));
            }
        }
    }

    public static class Job3bReducer extends Reducer<IntWritable, Couple, Couple, String> {

        @Override
        public void reduce(IntWritable key, Iterable<Couple> values, Context context)
                throws IOException, InterruptedException {

            int sumA = 0;
            ArrayList<Couple> arrValues = new ArrayList<Couple>();
            ArrayList<Couple> arrResults = new ArrayList<Couple>();

            for (Couple c : values) {
                arrValues.add(new Couple(new IntWritable(c.GetFirst().get()), new IntWritable(c.GetSecond().get())));
                sumA += c.GetSecond().get();
            }

            Collections.sort(arrValues);

            for (Couple v : arrValues) {
                if (!arrResults.isEmpty()) {
                    if (v.GetFirst().get() != arrResults.get(arrResults.size() - 1).GetFirst().get()) {
                        arrResults.add(
                                new Couple(new IntWritable(v.GetFirst().get()), new IntWritable(v.GetSecond().get())));
                    } else {
                        arrResults.get(arrResults.size() - 1).GetSecond()
                                .set(arrResults.get(arrResults.size() - 1).GetSecond().get() + v.GetSecond().get());
                    }
                } else
                    arrResults
                            .add(new Couple(new IntWritable(v.GetFirst().get()), new IntWritable(v.GetSecond().get())));
            }

            for (Couple c : arrResults) {
                Couple newKey = new Couple(key, c.GetFirst());
                context.write(newKey, new String(c.GetSecond().get() + "/" + sumA));
            }
        }
    }
}

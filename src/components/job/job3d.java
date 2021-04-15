package components.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import components.classes.*;

public class job3d {
    public static class Job3dMapper extends Mapper<Object, Text, Couple, Couple> {

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
                IntWritable c = new IntWritable(Integer.parseInt(arrItems[2]));
                IntWritable v = new IntWritable(Integer.parseInt(arrItems[3]));
                context.write(new Couple(b, c), new Couple(a, v));
                context.write(new Couple(a, c), new Couple(b, v));
                context.write(new Couple(a, b), new Couple(c, v));
            }
        }
    }

    public static class Job3dReducer extends Reducer<Couple, Couple, Triple, String> {

        @Override
        public void reduce(Couple key, Iterable<Couple> values, Context context)
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
                Triple newKey = new Triple(key.GetFirst(), key.GetSecond(), c.GetFirst());
                context.write(newKey, new String(c.GetSecond().get() + "/" + sumA));
            }
        }
    }
}

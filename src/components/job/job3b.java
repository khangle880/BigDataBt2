package components.job;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import components.classes.Couple;

public class job3b {
    public static class Job3bMapper extends Mapper<Object, Text, Couple, Couple> {

        IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] arrLines = value.toString().split("\n");

            for (String sLine : arrLines) {

                String[] arrItems = sLine.split(" ");

                int iLen = arrItems.length;

                ArrayList<Pair<Couple, Couple>> arrElement = new ArrayList<>();

                for (int i = 0; i < iLen - 1; i++) {
                    for (int j = i + 1; j < iLen; j++) {
                        if (!arrItems[i].equals(arrItems[j])) {
                            Couple cNew = new Couple(arrItems[i], arrItems[j]); // Tạo couple
                            if (!arrElement.isEmpty()) {
                                boolean bExist = false;
                                for (Pair<Couple, Couple> p : arrElement) {
                                    if (p.getFirst().compareTo(cNew) == 0) {
                                        bExist = true;
                                        break;
                                    }
                                }
                                if (!bExist)
                                    arrElement.add(
                                            new Pair<Couple, Couple>(cNew, new Couple(one, new IntWritable(iLen - 1))));
                            } else {
                                arrElement.add(
                                        new Pair<Couple, Couple>(cNew, new Couple(one, new IntWritable(iLen - 1))));
                            }
                        }
                    }
                }
                for (Pair<Couple, Couple> p : arrElement) {
                    context.write(p.getFirst(), p.getSecond());
                }
            }
        }
    }

    public static class Job3bReducer extends Reducer<Couple, Couple, Couple, FloatWritable> {

        FloatWritable result = new FloatWritable();

        @Override
        public void reduce(Couple key, Iterable<Couple> values, Context context)
                throws IOException, InterruptedException {

            int sumAB = 0;
            int sumA = 0;

            for (Couple val : values) {
                sumAB += val.GetFirst().get();
                sumA += val.GetSecond().get();
            }

            result.set((float) sumAB / sumA);
            context.write(key, result);
        }
    }
}

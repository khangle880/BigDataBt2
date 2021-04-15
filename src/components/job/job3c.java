package components.job;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import components.classes.Triple;

public class job3c {
    public static class Job3cMapper extends Mapper<Object, Text, Triple, IntWritable> {

        IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] arrLines = value.toString().split("\n");

            for (String sLine : arrLines) {

                String[] arrItems = sLine.split(" ");

                int iLen = arrItems.length;

                ArrayList<Triple> arrTriple = new ArrayList<>();

                for (int i = 0; i < iLen - 2; i++) {
                    for (int j = i + 1; j < iLen - 1; j++) {
                        for (int z = j + 1; z < iLen; z++) {

                            if ((!arrItems[i].equals(arrItems[j])) && (!arrItems[j].equals(arrItems[z]))
                                    && (!arrItems[i].equals(arrItems[z]))) {
                                Triple tNew = new Triple(arrItems[i], arrItems[j], arrItems[z]); 
                                if (!arrTriple.isEmpty()) {
                                    boolean bExist = false;
                                    for (Triple t : arrTriple) {
                                        if (t.compareTo(tNew) == 0) {
                                            bExist = true;
                                            break;
                                        }
                                    }
                                    if (!bExist)
                                        arrTriple.add(tNew);
                                } else {
                                    arrTriple.add(tNew);
                                }
                            }
                        }
                    }
                }
                for (Triple c : arrTriple) {
                    context.write(c, one);
                }
            }
        }
    }

    public static class Job3cReducer extends Reducer<Triple, IntWritable, Triple, IntWritable> {

        IntWritable result = new IntWritable();

        @Override
        public void reduce(Triple key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }
}

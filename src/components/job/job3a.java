package components.job;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import components.classes.Couple;

public class job3a {
    public static class Job3aMapper extends Mapper<Object, Text, Couple, IntWritable> {

        IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] arrLines = value.toString().split("\n");

            for (String sLine : arrLines) {

                String[] arrItems = sLine.split(" ");

                int iLen = arrItems.length;

                ArrayList<Couple> arrCouple = new ArrayList<>();

                for (int i = 0; i < iLen - 1; i++) {
                    for (int j = i + 1; j < iLen; j++) {

                        if (!arrItems[i].equals(arrItems[j])) {
                            Couple cNew = new Couple(arrItems[i], arrItems[j]); // Tạo couple
                            if (!arrCouple.isEmpty()) {
                                boolean bExist = false;
                                for (Couple c : arrCouple) {
                                    if (c.compareTo(cNew) == 0) {
                                        bExist = true;
                                        break;
                                    }
                                }
                                if (!bExist)
                                    arrCouple.add(cNew);
                            } else {
                                arrCouple.add(cNew);
                            }
                        }
                    }
                }
                for (Couple c : arrCouple) {
                    context.write(c, one);
                }
            }
        }
    }

        public static class Job3aReducer extends Reducer<Couple, IntWritable, Couple, IntWritable> {

        IntWritable result = new IntWritable();

        @Override
        public void reduce(Couple key, Iterable<IntWritable> values, Context context)
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

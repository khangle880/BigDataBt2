package components.classes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Couple implements WritableComparable<Couple> {
    private IntWritable iItem1;
    private IntWritable iItem2;

    public Couple() {
        iItem1 = new IntWritable(0);
        iItem2 = new IntWritable(0);
    }

    public Couple(IntWritable iItem1, IntWritable iItem2) {
        this.iItem1 = iItem1;
        this.iItem2 = iItem2;
    }

    public Couple(String sItem1, String sItem2) {
        this.iItem1 = new IntWritable(Integer.parseInt(sItem1));
        this.iItem2 = new IntWritable(Integer.parseInt(sItem2));
    }

    public IntWritable GetFirst() {
        return this.iItem1;
    }

    public IntWritable GetSecond() {
        return this.iItem2;
    }

    public void write(DataOutput dataOutput) throws IOException {
        iItem1.write(dataOutput);
        iItem2.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        iItem1.readFields(dataInput);
        iItem2.readFields(dataInput);
    }

    @Override
    public int compareTo(Couple otherCouple) {
        if (((this.iItem1.get() + this.iItem2.get()) == (otherCouple.iItem1.get() + otherCouple.iItem2.get()))
                && ((this.iItem1.compareTo(otherCouple.iItem1) == 0)
                        || (this.iItem1.compareTo(otherCouple.iItem2) == 0))) {
            return 0;
        } else if (this.iItem1.get() < otherCouple.iItem1.get()) {
            return -1;
        } else if (this.iItem1.get() > otherCouple.iItem1.get()) {
            return 1;
        } else if (this.iItem2.get() < otherCouple.iItem2.get()) {
            return -1;
        }
        return 1;
    }

    @Override
    public String toString() {
        return ("(" + this.iItem1.get() + " : " + this.iItem2.get() + ")");
    }
}
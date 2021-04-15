package components.classes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Triple implements WritableComparable<Triple> {
    private IntWritable iItem1;
    private IntWritable iItem2;
    private IntWritable iItem3;

    public Triple() {
        iItem1 = new IntWritable(0);
        iItem2 = new IntWritable(0);
        iItem3 = new IntWritable(0);
    }

    public Triple(IntWritable iItem1, IntWritable iItem2, IntWritable iItem3) {
        this.iItem1 = iItem1;
        this.iItem2 = iItem2;
        this.iItem3 = iItem3;
    }

    public Triple(String sItem1, String sItem2, String sItem3) {
        this.iItem1 = new IntWritable(Integer.parseInt(sItem1));
        this.iItem2 = new IntWritable(Integer.parseInt(sItem2));
        this.iItem3 = new IntWritable(Integer.parseInt(sItem3));
    }

    public IntWritable GetFirst() {
        return this.iItem1;
    }

    public IntWritable GetSecond() {
        return this.iItem2;
    }

    public IntWritable GetThird() {
        return this.iItem3;
    }

    public void write(DataOutput dataOutput) throws IOException {
        iItem1.write(dataOutput);
        iItem2.write(dataOutput);
        iItem3.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        iItem1.readFields(dataInput);
        iItem2.readFields(dataInput);
        iItem3.readFields(dataInput);
    }

    @Override
    public int compareTo(Triple otherTriple) {
        if (((this.iItem1.get() + this.iItem2.get() + this.iItem3.get()) == (otherTriple.iItem1.get()
                + otherTriple.iItem2.get() + otherTriple.iItem3.get()))
                && ((this.iItem1.compareTo(otherTriple.iItem1) == 0) || (this.iItem1.compareTo(otherTriple.iItem2) == 0)
                        || (this.iItem1.compareTo(otherTriple.iItem3) == 0))
                && ((this.iItem2.compareTo(otherTriple.iItem1) == 0) || (this.iItem2.compareTo(otherTriple.iItem2) == 0)
                        || (this.iItem2.compareTo(otherTriple.iItem3) == 0))) {
            return 0;
        } else if (this.iItem1.get() < otherTriple.iItem1.get()) {
            return -1;
        } else if (this.iItem1.get() > otherTriple.iItem1.get()) {
            return 1;
        } else if (this.iItem2.get() < otherTriple.iItem2.get()) {
            return -1;
        } else if (this.iItem2.get() > otherTriple.iItem2.get()) {
            return 1;
        } else if (this.iItem3.get() < otherTriple.iItem3.get()) {
            return -1;
        }
        return 1;
    }

    @Override
    public String toString() {
        return ("(" + this.iItem1.get() + " : " + this.iItem2.get() + " : " + this.iItem3.get() + ")");
    }
}
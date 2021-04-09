import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPairChi implements WritableComparable<TextPairChi> {

    private Text first;
    private DoubleWritable second;
//    private Text third;

    //Default constructor is a must
    public TextPairChi() {
        this.first=new Text();
        this.second=new DoubleWritable();
//        this.third = new Text();
    }

    public TextPairChi(Text first,DoubleWritable second) {
        try {
            this.first= first;
            this.second= second;
//            this.third = third;
        }catch(Exception ex) {
            System.out.println("Exception occurred "+ex.getCause());
        }

    }

// Other methods such as compare, equals, hashcode, write, readFields etc implementation also needs to done

    public Text getFirst() {
        return first;
    }

    public DoubleWritable getSecond() {
        return second;
    }

//    public Text getThird() {
//        return third;
//    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public void setSecond(DoubleWritable second) {
        this.second = second;
    }

//    public void setThird(Text third) {
//        this.third = third;
//    }

    @Override
    public String toString() {
        return this.first+"\t"+this.second+"\t";
//                + this.third + "\t";
    }


    @Override
    public int compareTo(TextPairChi o) {
        int compareValue = this.first.compareTo(o.getFirst());
        if (compareValue == 0) {
            compareValue = this.second.compareTo(o.getSecond());
        }
        return compareValue;

//        else
//        {
//            return third.compareTo(o.getThird());
//        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
//        third.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
//        third.readFields(dataInput);
    }
}
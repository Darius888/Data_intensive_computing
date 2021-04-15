import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPairChi implements WritableComparable<TextPairChi> {

    private Text first;
    private DoubleWritable second;


    public TextPairChi() {
        this.first = new Text();
        this.second = new DoubleWritable();
    }

    public TextPairChi(Text first, DoubleWritable second) {

        try {
            this.first = first;
            this.second = second;
        } catch (Exception ex) {
            System.out.println("Exception occurred " + ex.getCause());
        }

    }

    public Text getFirst() {
        return first;
    }

    public DoubleWritable getSecond() {
        return second;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public void setSecond(DoubleWritable second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return this.first + "\t" + this.second + "\t";
    }

    @Override
    public int compareTo(TextPairChi o) {
        int compareValue = this.first.compareTo(o.getFirst());
        if (compareValue == 0) {
            compareValue = this.second.compareTo(o.getSecond());
        }
        return compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }
}
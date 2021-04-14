import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    public TextPair() {
        this.first = new Text();
        this.second = new Text();

    }

    public TextPair(Text first, Text second) {
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

    public Text getSecond() {
        return second;
    }


    public void setFirst(Text first) {
        this.first = first;
    }

    public void setSecond(Text second) {
        this.second = second;
    }


    @Override
    public String toString() {
        return this.first + "\t" + this.second + "\t";

    }


    @Override
    public int compareTo(TextPair o) {
        if (o == null)
            return 0;
        int intcnt = first.compareTo(o.getFirst());
        int intcnt2 = second.compareTo(o.getSecond());
        if (intcnt != 0) {
            return intcnt;
        } else {
            return intcnt2;
        }

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReviewerIDAndCategoryModel implements WritableComparable<ReviewerIDAndCategoryModel> {

    private Text first;
    private Text second;

    public ReviewerIDAndCategoryModel() {
        this.first=new Text();
        this.second=new Text();

    }

    public ReviewerIDAndCategoryModel(Text first,Text second) {
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

    public Text getSecond() {
        return second;
    }

//    public Text getThird() {
//        return third;
//    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public void setSecond(Text second) {
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
    public int compareTo(ReviewerIDAndCategoryModel o) {
        if (o == null)
            return 0;
        int intcnt = first.compareTo(o.getFirst());
        int intcnt2 = second.compareTo(o.getSecond());
        if (intcnt != 0) {
            return intcnt;
        } else {
            return intcnt2;
        }
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
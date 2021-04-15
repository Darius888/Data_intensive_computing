import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortDoubleComparator extends WritableComparator {
    protected SortDoubleComparator() {
        super(TextPairChi.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TextPairChi k1 = (TextPairChi) w1;
        TextPairChi k2 = (TextPairChi) w2;


        int result = k1.getFirst().compareTo(k2.getFirst());
        if (0 == result) {
            result = -1 * k1.getSecond().compareTo(k2.getSecond());
        }

        return result;
    }
}
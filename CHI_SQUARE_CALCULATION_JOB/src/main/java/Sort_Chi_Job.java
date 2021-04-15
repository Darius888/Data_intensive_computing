import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * 11th job responsible for sorting chi square values in descending order.
 */
public class Sort_Chi_Job {

    public static class NaturalKeyGroupingComparator extends WritableComparator {

        protected NaturalKeyGroupingComparator() {
            super(TextPairChi.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextPairChi k1 = (TextPairChi) w1;
            TextPairChi k2 = (TextPairChi) w2;

            return k1.getSecond().compareTo(k2.getSecond());

        }
    }

    public static class NaturalKeyPartitioner extends Partitioner<TextPairChi, Text> {

        @Override
        public int getPartition(TextPairChi key, Text val, int numPartitions) {
            return (key.getSecond().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static class TokenizerMapper
            extends Mapper<Object, Text, TextPairChi, Text> {

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method each line is received as "category term chi value"
         * Then this line is splitted through whitespaces.
         * Then such key value pairs are emitted < key: (category, chi value) value: term >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            context.write(new TextPairChi(new Text(split[0]), new DoubleWritable(Double.parseDouble(split[2]))), new Text(split[1]));

        }
    }

    public static class IntSumReducer
            extends Reducer<TextPairChi, Text, Text, Text> {
        long counter = 0;
        Text prev = null;


        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key: (category, chi value) value: term > which are sorted in descending order, by using partitioner, sort comparator, natural key grouping comparator.
         * Then for each key(category) counter is added to identify the position so in the next job we could take top 150 values.
         * Then we output such key values pairs < key:category term, value:chi^2 value counter >
         */
        public void reduce(TextPairChi key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text text : values) {
                if (prev != null) {
                    if (!key.getFirst().toString().equals(prev.toString())) {
                        counter = 0;
                    }
                }

                context.write(new Text(key.getFirst().toString()), new Text(text.toString() + " " + key.getSecond().toString() + " " + counter));
                counter++;
                prev = new Text(key.getFirst().toString());
            }
        }
    }
}

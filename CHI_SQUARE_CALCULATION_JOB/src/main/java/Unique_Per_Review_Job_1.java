import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Second job responsible for outputting distinct terms per each review
 */
public class Unique_Per_Review_Job_1 {

    public static class Mapper1
            extends Mapper<Object, Text, Text, NullWritable> {

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method each line is received as "reviewID category term 1"
         * Then this line is splitted through whitespaces.
         * Then such key values pairs are emitted through mapper: < key:(reviewID category term 1),value:NullWritable >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] keyVal1 = value.toString().split("\\s+");

            context.write(new Text(keyVal1[0] + " " + keyVal1[1] + " " + keyVal1[2] + " " + keyVal1[3]), NullWritable.get());

        }
    }

    public static class Reducer1
            extends Reducer<Text, Text, Text, NullWritable> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key:(reviewID category term 1),value:NullWritable >
         * Then such key values pairs are emitted through reducer: < key:(reviewID category term 1), value:NullWritable >
         * BY USING NULLWRITABLE, DUPLICATES ARE REMOVED.
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(new Text(key), NullWritable.get());
        }
    }
}

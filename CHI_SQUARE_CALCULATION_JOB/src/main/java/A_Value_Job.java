import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Third job responsible for calculating A value, which is number of review texts per category, which contain term, needed for chi^2 calculation
 */
public class A_Value_Job {

    public static class Mapper1
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method each line is received as "reviewID category term 1"
         * Then this line is splitted through whitespaces.
         * Then such key values pairs are emitted through mapper: < key:(category term),value:1 >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] splited = value.toString().split("\\s+");

            Text val = new Text(splited[1] + " " + splited[2]);
            context.write(val, ONE);

        }
    }


    public static class Reducer1
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key:(category term),value:1 >
         * Then such key values pairs are emitted through reducer: < key:(category term), value:sum(of reviews per category containing term) >
         */
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable model : values) {
                sum += model.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

}

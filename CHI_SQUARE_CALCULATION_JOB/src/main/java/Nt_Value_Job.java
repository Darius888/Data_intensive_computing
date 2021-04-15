import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Fourth job responsible for calculating Nt value, which is number of review texts, which contain term.
 */
public class Nt_Value_Job {

    public static class Mapper1
            extends Mapper<Object, Text, Text, IntWritable> {

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method each line is received as "category term sum(of review per category containing term)"
         * Then this line is splitted through whitespaces.
         * Then such key values pairs are emitted through mapper: < key:(term),value:sum(of reviews per category containing term) >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] splited = value.toString().split("\\s+");

            context.write(new Text(splited[1]), new IntWritable(Integer.parseInt(splited[2])));

        }
    }

    public static class Reducer1
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key:(term),value:sum(of review per category containing term) >
         * Then for values(sum(of review per category containing term)) are summed up for each term
         * Then such key values pairs are emitted through reducer: < key:(term), value:sum(number of review texts, which contain term) >
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

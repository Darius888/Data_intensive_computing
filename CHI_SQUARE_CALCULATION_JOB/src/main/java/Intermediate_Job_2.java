import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


/**
 * Eight job responsible for calculating total number of records N.
 */
public class Intermediate_Job_2 {

    public static class Mapper1
            extends Mapper<Object, Text, Text, Text> {

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method each line is received as "category sum(number of review texts per category)
         * Then such key values pairs are emitted through mapper: < key:N,value:sum(number of review texts per category) category >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            context.write(new Text("N"), new Text(split[1] + " " + split[0]));

        }
    }


    public static class Reducer1
            extends Reducer<Text, Text, Text, Text> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from reducer < key:N,value:sum(number of review texts per category) category >
         * Then each value is splitted and sum(number of review texts per category) is summed up together per all categories
         * Then for each category such key value pair is emitted < key: category, value: N >
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Text> result = new ArrayList<Text>();

            int sum = 0;

            for (Text text : values) {
                String[] split = text.toString().split("\\s+");
                sum += Integer.parseInt(split[0].toString());
                result.add(new Text(split[1]));
            }

            for (Text text : result) {
                context.write(text, new Text(String.valueOf(sum)));
            }
        }
    }
}

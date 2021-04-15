import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeSet;


/**
 * 12th job responsible for taking top 150 values.
 */
public class Top150_Job {

    public static class Mapper1
            extends Mapper<Object, Text, IntWritable, Text> {

        long counteris = 0;
        String prev = "";

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method each line is received as category term value:chi^2 value counter
         * Then this line is splitted through whitespaces.
         * Then such key value pairs are emitted if the counter is less than 150 < key: counter(0) value: category term chi^2 value counter>
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            if (Integer.parseInt(split[3]) < 150) {
                context.write(new IntWritable((int) counteris), new Text(value));
            }

            prev = split[0];
        }
    }

    public static class Reducer1
            extends Reducer<IntWritable, Text, Text, Text> {

        String prev = "";
        StringBuilder stringBuilder = new StringBuilder();
        ArrayList<Text> arrayList = new ArrayList<>();
        TreeSet<Text> treeSet = new TreeSet<Text>();

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key: counter(0) value: category term chi^2 value counter>
         * Then for each value we take only those which have position identifier < 150.
         * Then we reverse the arrayList and combine for each category all terms and their values into one line and output such key value pairs: < key : category, value: term1:chi^2 value, term2:chi^2 value ... >
         * On cleanup method we emit dictionary containing all top terms.
         */
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            for (Text text : values) {
                treeSet.add(new Text(text.toString().split("\\s+")[1]));
                if (Integer.parseInt(text.toString().split("\\s+")[3]) <= 150) {
                    arrayList.add(new Text(text));
                }
            }

            Collections.reverse(arrayList);

            for (int i = 0; i < arrayList.size(); i++) {

                if ((!arrayList.get(i).toString().split("\\s+")[0].equals(prev) && !prev.isEmpty()) || arrayList.size() - 1 == i) {
                    context.write(new Text(prev), new Text(stringBuilder.toString()));
                    stringBuilder = new StringBuilder();
                }

                if (Integer.parseInt(arrayList.get(i).toString().split("\\s+")[3]) < 150) {
                    stringBuilder.append(arrayList.get(i).toString().split("\\s+")[1] + ":" + arrayList.get(i).toString().split("\\s+")[2] + " ");
                }
                prev = arrayList.get(i).toString().split("\\s+")[0];
            }

        }
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();

            for (Text t : treeSet) {
                stringBuilder.append(t + " ");

            }
            context.write(new Text(stringBuilder.toString()), new Text());
        }
    }
}


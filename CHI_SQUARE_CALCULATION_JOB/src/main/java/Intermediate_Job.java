import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


/**
 * Sixth job responsible for combining A and Nt values into one file.
 */
public class Intermediate_Job {

    public static class Mapper1
            extends Mapper<Object, Text, Text, Text> {

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method, from one file, each line is received as "category term sum(of review per category containing term)" and from another "term sum(number of review texts, which contain term)"
         * Then this line is splitted through whitespaces.
         * Then by length we identify from which file the line is coming and based on that we output two different keys
         * If length == 3, then such key values pairs are emitted through mapper: < key:category,value:term 1 >
         * If length == 2, then such key values pairs are emitted through mapper: < key:category,value:term >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");
            Text newKey = new Text();
            Text newVal = new Text();
            if (split.length == 3) {
                newKey = new Text(split[1]);
                newVal = new Text(split[0] + " " + split[2]);

                context.write(newKey, newVal);

            }
            if (split.length == 2) {
                newKey = new Text(split[0]);
                newVal = new Text(split[1]);

                context.write(newKey, newVal);
            }

        }
    }

    public static class Combiner
            extends Reducer<Text, Text, Text, Text> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key:category,value:term 1 > , < key:category,value:term >
         * Then it is checked if value for each key(category) splits into two or one strings
         * If it's 1, then we know that it's sum(number of review texts, which contain term) and we add it to one array
         * If 2 then we know that's term and sum(of review per category containing term)
         * Then we combine all and output such key values pairs < key:category, value:term A Nt >
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Text newVal = new Text();
            Text aaa = new Text();
            ArrayList<Text> texts = new ArrayList<>();
            ArrayList<Text> valueszz = new ArrayList<>();

            for (Text val : values) {

                if (val.toString().split("\\s+").length == 1) {
                    aaa = new Text(val);
                }
                if (val.toString().split("\\s+").length == 2) {
                    texts.add(new Text(val));

                }
            }
            for (Text text : texts) {
                newVal = new Text(key + " " + text.toString().split("\\s+")[1]);
                valueszz.add(new Text(text.toString().split("\\s+")[0] + " " + newVal + " " + aaa.toString()));
            }

            for (Text text : valueszz) {
                context.write(new Text(text.toString().split("\\s+")[0]), new Text(text.toString().split("\\s+")[1] + " " + text.toString().split("\\s+")[2] + " " + text.toString().split("\\s+")[3]));
            }
        }
    }
}

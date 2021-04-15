import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Seventh job responsible for combining A, Nt and Nc values into one file.
 */
public class Intermediate_Job_1 {

    public static class Mapper1
            extends Mapper<Object, Text, Text, Text> {

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method, from one file, each line is received as "category term A Nt" and from another "category Nc"
         * Then this line is splitted through whitespaces.
         * Then by length we identify from which file the line is coming and based on that we output two different keys
         * If length == 4, then such key values pairs are emitted through mapper: < key:category,value:term A Nt >
         * If length == 2, then such key values pairs are emitted through mapper: < key:category,value:Nc >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");
            Text newKey = new Text();
            Text newVal = new Text();
            if (split.length == 4) {

                context.write(new Text(split[0]), new Text(split[1] + " " + split[2] + " " + split[3]));
            }

            if (split.length == 2) {
                newKey = new Text(split[0]);
                newVal = new Text(split[1]);
                context.write(newKey, newVal);
            }
        }
    }

    public static class Reducer1
            extends Reducer<Text, Text, Text, Text> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key:category,value:term A Nt > , < key:category,value:Nc >
         * Then it is checked if value for each key(category) splits into three or one strings
         * If it's 1, then we know that it's Nc and we add it to one array
         * If 3 then we know that's term A Nt
         * Then we combine all and output such key values pairs < key:category, value:term A Nt Nc>
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Text tmp3 = new Text();
            ArrayList<Text> arrayList = new ArrayList<>();
            ArrayList<Text> finalArrayList = new ArrayList<>();
            for (Text value : values) {
                if (value.toString().split("\\s+").length == 1) {
                    tmp3 = new Text(value);
                }
                if (value.toString().split("\\s+").length == 3) {
                    arrayList.add(new Text(value));

                }

            }

            for (Text text : arrayList) {
                finalArrayList.add(new Text(text.toString() + " " + tmp3.toString()));
            }

            for (Text text : finalArrayList) {
                context.write(key, text);
            }
        }
    }
}

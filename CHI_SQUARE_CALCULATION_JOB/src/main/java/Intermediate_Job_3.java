import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


/**
 * Ninth job responsible for combining A Nt Nc and N values together which are all needed to get B,C,D values for chi square calculation.
 */
public class Intermediate_Job_3 {

    public static class Mapper1
            extends Mapper<Object, Text, Text, Text> {


        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method, from one file, each line is received as "category term A Nt Nc" and from another "category N"
         * Then this line is splitted through whitespaces.
         * Then by length we identify from which file the line is coming and based on that we output two different keys
         * If length == 5, then such key values pairs are emitted through mapper: < key:category,value:term A Nt Nc>
         * If length == 2, then such key values pairs are emitted through mapper: < key:category,value:N >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            if (split.length == 2) {
                context.write(new Text(split[0]), new Text(split[1]));
            }

            if (split.length == 5) {
                context.write(new Text(split[0]), new Text(split[1] + " " + split[2] + " " + split[3] + " " + split[4]));
            }
        }
    }

    public static class Reducer1
            extends Reducer<Text, Text, Text, Text> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key:category,value:term A Nt Nc> , < key:category,value:N >
         * Then it is checked if value for each key(category) splits into four or one strings
         * If it's 1, then we know that it's N and we add it to one array
         * If 4 then we know that's term A Nt Nc and add it to another
         * Then we combine all and output such key values pairs < key:category, value:term A Nt Nc N>
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            ArrayList<Text> arrayList = new ArrayList<>();
            ArrayList<Text> finalArrayList = new ArrayList<>();
            Text tmp3 = new Text();


            for (Text value : values) {
                if (value.toString().split("\\s+").length == 1) {
                    tmp3 = new Text(value);
                }
                if (value.toString().split("\\s+").length == 4) {
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

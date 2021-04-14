import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class Intermediate_Job_3 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {


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

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {


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

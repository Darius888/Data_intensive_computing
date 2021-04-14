import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class Intermediate_Job_1 {


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {


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


    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

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

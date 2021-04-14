import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class Intermediate_Job_2 {


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            context.write(new Text("N"), new Text(split[1] + " " + split[0]));

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Text> result = new ArrayList<Text>();

            int sum = 0;

            for (Text intWritable : values) {
                String[] split = intWritable.toString().split("\\s+");
                sum += Integer.parseInt(split[0].toString());
                result.add(new Text(split[1]));

            }
            for (Text text : result) {
                context.write(text, new Text(String.valueOf(sum)));
            }

        }
    }


}

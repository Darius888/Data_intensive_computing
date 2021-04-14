import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Dictionary_Job {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            context.write(new Text(split[0]), new Text("fKey"));

        }
    }

    public static class Combiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            for (Text text : values) {
                context.write(new Text(text), new Text(key));

            }


        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            ArrayList<Text> texts = new ArrayList<>();
            for (Text text : values) {
                texts.add(new Text(text));
            }
            StringBuilder stringBuilder = new StringBuilder();
            for (Text text : texts) {
                stringBuilder.append(text.toString() + " ");
            }

            context.write(new Text(""), new Text(stringBuilder.toString()));

        }
    }
}

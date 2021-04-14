import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class Intermediate_Job {


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {


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
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Text finalVal = new Text();
            Text newVal = new Text();
            Text aaa = new Text();
            Text newKey = new Text();
            ArrayList<Text> texts = new ArrayList<>();
            ArrayList<Text> valueszz = new ArrayList<>();
            ArrayList<Text> keyzz = new ArrayList<>();

            for (Text val : values) {

                if (val.toString().split("\\s+").length == 1) {
                    aaa = new Text(val);
                }
                if (val.toString().split("\\s+").length == 2) {
                    texts.add(new Text(val));

                }
            }
            //newVal CONSISTS OF TWO VALUES!!!!!!!
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

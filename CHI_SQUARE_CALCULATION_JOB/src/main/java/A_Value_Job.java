import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class A_Value_Job {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] splited = value.toString().split("\\s+");

            Text val = new Text(splited[1] + " " + splited[2]);
            context.write(val, ONE);

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable model : values) {
                sum += model.get();

            }
            context.write(key, new IntWritable(sum));
        }
    }

}

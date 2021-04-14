import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Unique_Per_Review_Job_1 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, NullWritable> {

        Text prev = new Text(" ");
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] keyVal1 = value.toString().split("\\s+");


                context.write(new Text(keyVal1[0] + " " + keyVal1[1] + " " + keyVal1[2] + " " + keyVal1[3]), NullWritable.get());

        }
    }

    public static class Reducerz
            extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {


              context.write(new Text(key), NullWritable.get());
        }
    }
}

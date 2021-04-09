import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;

public class Nc_Value_Job {


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text term = new Text();

        FileArrayProvider provider = new FileArrayProvider();
        List<String> lines;

        {
            try {
                lines = provider.readLines("stopwords.txt");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            JSONObject obj = new JSONObject(value.toString());
            Text category = new Text(obj.getString("category"));
            Text reviewID = new Text(obj.getString("reviewerID"));
            term = new Text(obj.getString("reviewText"));


            context.write(category, ONE);


        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {


            int sum = 0;
            for(IntWritable val: values)
            {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));

        }
    }



}

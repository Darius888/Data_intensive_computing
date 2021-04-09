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
import java.util.StringTokenizer;

public class Unique_per_review_Job {


    public static class TokenizerMapper
            extends Mapper<Object, Text, ReviewerIDAndCategoryModel, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text term = new Text();

        CustomWritableValue  customWritableValue = new CustomWritableValue();
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


            StringTokenizer itr = new StringTokenizer(term.toString(),  " .!?,;:()[]{}-_\"'`~#&*%$\\/");

            while (itr.hasMoreTokens()) {
                String term_token = itr.nextToken().toLowerCase();
                term_token = term_token
                        .replaceAll("[0-9]", "")
                        .replaceAll("\\s+","");
                if (term_token.length() > 1 && !lines.contains(term_token))
                {
//                    term.set(term_token + "=" + ONE);


                    context.write(new ReviewerIDAndCategoryModel(reviewID, new Text(category+":"+term_token)), ONE);
                }
            }
        }
    }

    public static class Combinatorika
            extends Reducer<ReviewerIDAndCategoryModel, IntWritable, ReviewerIDAndCategoryModel, IntWritable> {

        public void reduce(ReviewerIDAndCategoryModel key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            String[] keyVal1 = key.getSecond().toString().split(":");

            ReviewerIDAndCategoryModel newKey = new ReviewerIDAndCategoryModel(new Text(keyVal1[0]),new Text(keyVal1[1]));
            context.write(newKey, new IntWritable(1));

        }
    }

    public static class IntSumReducer
            extends Reducer<ReviewerIDAndCategoryModel, IntWritable, ReviewerIDAndCategoryModel, IntWritable> {

        public void reduce(ReviewerIDAndCategoryModel key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

            String aha = "";
//            System.out.println(key.toString());


//            System.out.println(key.toString() + sum);


//               context.write(key,new Text(aha));



        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Chi value");
        job.setNumReduceTasks(2);
        job.setJarByClass(Unique_per_review_Job.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(Combiner.Combinatorika.class);
        job.setReducerClass(Combinatorika.class);
        job.setMapOutputKeyClass(ReviewerIDAndCategoryModel.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(ReviewerIDAndCategoryModel.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

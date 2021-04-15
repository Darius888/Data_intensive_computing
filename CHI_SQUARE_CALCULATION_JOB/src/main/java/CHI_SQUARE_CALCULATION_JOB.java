import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Main driver class for chaining all jobs together
 */
public class CHI_SQUARE_CALCULATION_JOB {

    public static void main(String[] args) throws Exception {

        Path out = new Path("tmpoutput");

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Unique per review Job part 1");
        job1.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job1.setMapperClass(Unique_per_review_Job.Mapper1.class);
        job1.setNumReduceTasks(0);
        job1.setOutputKeyClass(TextPair.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job20 = Job.getInstance(conf, "Unique per review Job part 2");
        job20.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job20.setMapperClass(Unique_Per_Review_Job_1.Mapper1.class);
        job20.setCombinerClass(Unique_Per_Review_Job_1.Reducer1.class);
        job20.setReducerClass(Unique_Per_Review_Job_1.Reducer1.class);
        job20.setNumReduceTasks(30);
        job20.setMapOutputKeyClass(Text.class);
        job20.setMapOutputValueClass(NullWritable.class);
        job20.setOutputKeyClass(Text.class);
        job20.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job20, new Path(out, "out1"));
        FileOutputFormat.setOutputPath(job20, new Path(out, "out2"));

        if (!job20.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Job for A");
        job2.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job2.setMapperClass(A_Value_Job.Mapper1.class);
        job2.setReducerClass(A_Value_Job.Reducer1.class);
        job2.setNumReduceTasks(30);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(out, "out2"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out3"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job3 = Job.getInstance(conf, "Job for Nt");
        job3.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job3.setMapperClass(Nt_Value_Job.Mapper1.class);
        job3.setReducerClass(Nt_Value_Job.Reducer1.class);
        job3.setNumReduceTasks(30);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(out, "out3"));
        FileOutputFormat.setOutputPath(job3, new Path(out, "out4"));

        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job4 = Job.getInstance(conf, "Job for Nc");
        job4.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job4.setMapperClass(Nc_Value_Job.Mapper1.class);
        job4.setReducerClass(Nc_Value_Job.Reducer1.class);
        job4.setNumReduceTasks(30);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(IntWritable.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[0]));
        FileOutputFormat.setOutputPath(job4, new Path(out, "out5"));

        if (!job4.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job5 = Job.getInstance(conf, "Job for combining A ant Nt");
        job5.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job5.setMapperClass(Intermediate_Job.Mapper1.class);
        job5.setReducerClass(Intermediate_Job.Combiner.class);
        job5.setNumReduceTasks(30);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job5, new Path(out, "out3"),
                TextInputFormat.class, Intermediate_Job.Mapper1.class);
        MultipleInputs.addInputPath(job5, new Path(out, "out4"),
                TextInputFormat.class, Intermediate_Job.Mapper1.class);
        FileOutputFormat.setOutputPath(job5, new Path(out, "out6"));

        if (!job5.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job6 = Job.getInstance(conf, "Job for combining A ant Nt and Nc");
        job6.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job6.setMapperClass(Intermediate_Job_1.Mapper1.class);
        job6.setReducerClass(Intermediate_Job_1.Reducer1.class);
        job6.setNumReduceTasks(30);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(Text.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job6, new Path(out, "out5"),
                TextInputFormat.class, Intermediate_Job_1.Mapper1.class);
        MultipleInputs.addInputPath(job6, new Path(out, "out6"),
                TextInputFormat.class, Intermediate_Job_1.Mapper1.class);
        FileOutputFormat.setOutputPath(job6, new Path(out, "out7"));

        if (!job6.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job7 = Job.getInstance(conf, "Job for calculating N");
        job7.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job7.setMapperClass(Intermediate_Job_2.Mapper1.class);
        job7.setReducerClass(Intermediate_Job_2.Reducer1.class);
        job7.setNumReduceTasks(30);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job7, new Path(out, "out5"));
        FileOutputFormat.setOutputPath(job7, new Path(out, "out8"));

        if (!job7.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job8 = Job.getInstance(conf, "Job for combining A Nt Nc N values together for category and term");
        job8.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job8.setMapperClass(Intermediate_Job_3.Mapper1.class);
        job8.setReducerClass(Intermediate_Job_3.Reducer1.class);
        job8.setNumReduceTasks(30);
        job8.setMapOutputKeyClass(Text.class);
        job8.setMapOutputValueClass(Text.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job8, new Path(out, "out7"),
                TextInputFormat.class, Intermediate_Job_3.Mapper1.class);
        MultipleInputs.addInputPath(job8, new Path(out, "out8"),
                TextInputFormat.class, Intermediate_Job_3.Mapper1.class);
        FileOutputFormat.setOutputPath(job8, new Path(out, "out9"));

        if (!job8.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job9 = Job.getInstance(conf, "Job for calculating Chi values");
        job9.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job9.setMapperClass(Chi_Value_Job.Mapper1.class);
        job9.setReducerClass(Chi_Value_Job.Reducer1.class);
        job9.setNumReduceTasks(30);
        job9.setMapOutputKeyClass(Text.class);
        job9.setMapOutputValueClass(Text.class);
        job9.setOutputKeyClass(TextPair.class);
        job9.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job9, new Path(out, "out9"));
        FileOutputFormat.setOutputPath(job9, new Path(out, "out10"));

        if (!job9.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job10 = Job.getInstance(conf, "Sort chi values Job");
        job10.setNumReduceTasks(1);
        job10.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job10.setMapperClass(Sort_Chi_Job.TokenizerMapper.class);
        job10.setReducerClass(Sort_Chi_Job.IntSumReducer.class);
        job10.setPartitionerClass(Sort_Chi_Job.NaturalKeyPartitioner.class);
        job10.setGroupingComparatorClass(Sort_Chi_Job.NaturalKeyGroupingComparator.class);
        job10.setSortComparatorClass(SortDoubleComparator.class);

        job10.setMapOutputKeyClass(TextPairChi.class);
        job10.setMapOutputValueClass(Text.class);
        job10.setOutputKeyClass(Text.class);
        job10.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job10, new Path(out, "out10"));
        FileOutputFormat.setOutputPath(job10, new Path(out, "out11"));
        if (!job10.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job15 = Job.getInstance(conf, "Pick Top 150 values Job");
        job15.setJarByClass(CHI_SQUARE_CALCULATION_JOB.class);
        job15.setMapperClass(Top150_Job.Mapper1.class);
        job15.setReducerClass(Top150_Job.Reducer1.class);
        job15.setNumReduceTasks(30);
        job15.setMapOutputKeyClass(IntWritable.class);
        job15.setMapOutputValueClass(Text.class);
        job15.setOutputKeyClass(Text.class);
        job15.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job15, new Path(out, "out11"));
        FileOutputFormat.setOutputPath(job15, new Path(args[1]));

        if (!job15.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;



public class Top150_Job {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");
            context.write(new Text(split[0]), new Text(split[1] + "  " + split[2]));

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        int count = 0;
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

                ArrayList<Text> texts = new ArrayList<>();

                for(Text text: values)
                {
                    texts.add(new Text(text));

                }
                StringBuilder stringBuilder = new StringBuilder();
                int count=0;
                for(Text text: texts)
                {

                    if(count  < 150)
                    {
                        //
                        stringBuilder.append(text.toString().split("\\s+")[0] + ":" + text.toString().split("\\s+")[1] + " ");
                    }
                    count++;
                }

                System.out.println("AHA");
            context.write(key,new Text(stringBuilder.toString()));


        }
    }
}

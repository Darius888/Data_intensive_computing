import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

import java.io.IOException;


/**
 * Fifth job responsible for calculating Nc, which is number of documents per category.
 */
public class Nc_Value_Job {

    public static class Mapper1
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable ONE = new IntWritable(1);

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method each line is received as a json: {"reviewerID": "A2VNYWOPJ13AFP", "asin": "0981850006", "reviewerName": "Amazon Customer \"carringt0n\"", "helpful": [6, 7], "reviewText": "This was a gift for my other husband.  He's making us things from it all the time and we love the food.  Directions are simple, easy to read and interpret, and fun to make.  We all love different kinds of cuisine and Raichlen provides recipes from everywhere along the barbecue trail as he calls it. Get it and just open a page.  Have at it.  You'll love the food and it has provided us with an insight into the culture that produced it. It's all about broadening horizons.  Yum!!", "overall": 5.0, "summary": "Delish", "unixReviewTime": 1259798400, "reviewTime": "12 3, 2009", "category": "Patio_Lawn_and_Garde"}
         * Then from the json "category" field is extracted.
         * Then such key values pairs are emitted through reducer: < key:category, value:1 >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            JSONObject obj = new JSONObject(value.toString());
            Text category = new Text(obj.getString("category"));
            Text reviewerID = new Text(obj.getString("reviewerID"));

            context.write(category, reviewerID);
        }
    }

    public static class Reducer1
            extends Reducer<Text, Text, Text, LongWritable> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key:(category),value:1 >
         * Then each value per category is summed up
         * Then such key values pairs are emitted through mapper: < key:(category), value:sum(number of documents per category) >
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            long sum = 0;
            for (Text val : values) {
//                sum += val.get();
                sum +=1;
            }
            context.write(key, new LongWritable(sum));

        }
    }
}

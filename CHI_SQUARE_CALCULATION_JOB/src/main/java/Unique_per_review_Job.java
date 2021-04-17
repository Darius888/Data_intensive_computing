import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;


/**
 * First job responsible for outputting value A (number of review texts which contain term t in category c)
 */
public class Unique_per_review_Job {


    /**
     * Mapper class in which map method is as well as a list of stopwords added as a List.
     */
    public static class Mapper1
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        List<String> lines = Arrays.asList(
                "a", "aa", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "ain", "all", "allow", "allows"
                , "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything"
                , "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "aren", "around", "as", "aside", "ask", "asking", "associated", "at", "available"
                , "away", "awfully", "b", "bb", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside"
                , "besides", "best", "better", "between", "beyond", "bibs", "book", "both", "brief", "but", "by", "c", "came", "can", "cannot", "cant", "car", "cause", "causes", "cd", "certain"
                , "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding"
                , "could", "couldn", "course", "currently", "d", "definitely", "described", "despite", "did", "didn", "different", "do", "does", "doesn", "doing", "don", "done", "down", "downwards"
                , "during", "e", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything"
                , "everywhere", "ex", "exactly", "example", "except", "f", "far", "few", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four"
                , "from", "further", "furthermore", "g", "game", "game", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "h", "had", "hadn"
                , "happens", "hardly", "has", "hasn", "have", "haven", "having", "he", "hello", "help", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself"
                , "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate"
                , "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isn", "it", "its", "itself", "j", "just", "k", "keep", "keeps", "kept", "know", "known", "knows"
                , "l", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "life", "like", "liked", "likely", "little", "ll", "look", "looking", "looks", "ltd", "m", "mainly"
                , "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "mon", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "n", "name", "namely", "nd", "near"
                , "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel"
                , "now", "nowhere", "o", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours"
                , "ourselves", "out", "outside", "over", "overall", "own", "p", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides"
                , "q", "que", "quite", "qv", "r", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "s", "said", "same", "saw", "say"
                , "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall"
                , "she", "should", "shouldn", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify"
                , "specifying", "still", "sub", "such", "sup", "sure", "t", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "that", "thats", "the", "their", "theirs", "them", "themselves"
                , "then", "thence", "there", "there", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "think", "third", "this", "thorough", "thoroughly", "those", "though"
                , "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "u", "un", "under", "unfortunately"
                , "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "v", "value", "various", "ve", "very", "via", "viz", "vs", "want", "wants", "was", "wasn"
                , "way", "we", "welcome", "well", "went", "were", "weren", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether"
                , "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "won", "wonder", "would", "wouldn", "x", "y", "yes", "yet", "you"
                , "your", "yours", "yourself", "yourselves", "z", "zero"
        );

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method each line is received as a json: {"reviewerID": "A2VNYWOPJ13AFP", "asin": "0981850006", "reviewerName": "Amazon Customer \"carringt0n\"", "helpful": [6, 7], "reviewText": "This was a gift for my other husband.  He's making us things from it all the time and we love the food.  Directions are simple, easy to read and interpret, and fun to make.  We all love different kinds of cuisine and Raichlen provides recipes from everywhere along the barbecue trail as he calls it. Get it and just open a page.  Have at it.  You'll love the food and it has provided us with an insight into the culture that produced it. It's all about broadening horizons.  Yum!!", "overall": 5.0, "summary": "Delish", "unixReviewTime": 1259798400, "reviewTime": "12 3, 2009", "category": "Patio_Lawn_and_Garde"}
         * Then from the json "category", "reviewText" fields are extracted.
         * After that iteration through review text is done and not needed symbols are filtered out as well as digits, single characters and stopwords which are described in the List lines.
         * Then all terms per each category, per each review are added to hash set to eliminate duplicates
         * Then such key values pairs are emitted through mapper: < key:(category term),value:1 >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            HashSet<Text> texts =new HashSet<>();

            JSONObject obj = new JSONObject(value.toString());
            Text category = new Text(obj.getString("category"));
            Text term = new Text(obj.getString("reviewText"));

            StringTokenizer itr = new StringTokenizer(term.toString(), " \t0123456789.!?,;:()[]{}-_\"'`~#&*%$\\/");

            while (itr.hasMoreTokens()) {
                String term_token = itr.nextToken().toLowerCase();
                term_token = term_token
                        .replaceAll("\\s+","")
                        .replaceAll("\t", "");

                if (term_token.length() > 1 && !lines.contains(term_token)) {
                    texts.add(new Text(term_token));
                }
            }
            for(Text text: texts)
            {
                context.write(new Text(category + "  " + text.toString()), ONE);
            }
        }
    }


    public static class Reducer1
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key:(category term),value:1 >
         * Then for each category we sum how many terms we have in each category per each review
         * We emit such key value pairs < key: category term, value: sum(number of review texts which contain term t in category c)
         */
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable text:values)
            {
                sum += text.get();
            }
            context.write(new Text(key), new IntWritable(sum));
        }
    }

}

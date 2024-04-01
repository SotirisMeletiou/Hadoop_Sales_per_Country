import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        

public class Sales {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private Text country = new Text();
	    private final static IntWritable sales = new IntWritable();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] tokens = line.split(",");
	        
	        if (tokens.length >= 8) {
	            String countryName = tokens[7].trim();
	            int salesAmount = Integer.parseInt(tokens[2].trim());

	            country.set(countryName);
	            sales.set(salesAmount);

	            context.write(country, sales);
	        }
	    }
	}



	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	            throws IOException, InterruptedException {
	        int sum = 0;
	        int count = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	            count++;
	        }
	        Text res= new Text();
	        res.set(count+" "+sum);
	       
	        context.write(key, res);
	    }
	}




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "sales");
        job.setJarByClass(Sales.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/csdeptucy/input/SalesJan2009.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/csdeptucy/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Filter {
	
	
	
//	public static class FilterMapper extends Mapper<Object, Text, Object, Text> {
//		
//		@Override
//		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
//			
//			String filterValue = context.getConfiguration().get("filtervalue");
//			
//			if(value.toString().contains(filterValue))
//				context.write(key,value);
//			
//		}
//		
//	
//	}
	
	public static class FilterMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable ONE = new IntWritable(1);
		
		@Override
		protected void map(Object key, Text value, Mapper<Object,Text,Text,IntWritable>.Context context) throws IOException, InterruptedException{
			
			String keyword = context.getConfiguration().get("filterkeyword");
			System.out.println(keyword);
			
			if(value.toString().contains(keyword)){
				Text newKey = new Text(keyword);
				context.write(newKey,ONE);
			}
			
		}
		
	
	}
	
	public static class FilterReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private IntWritable result = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
			
			
			int sum = 0;
			
			for(IntWritable val : values){
				sum  += val.get();
			}
			
			
			result.set(sum);
			
			context.write(key,result);
			
		}
	}


	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
	
		
		conf.set("filterkeyword",args[2]);

		Job job = Job.getInstance(conf, "filter");
		
		job.setJarByClass(Filter.class);
		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

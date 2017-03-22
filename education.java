import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class education {
	public static class edumap extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] parts= value.toString().split(",");
		 IntWritable one=new IntWritable(1);
		if(parts != null)
		{
		//String age=parts[0];
		String token[]=parts[0].split(":");
		
		String Education=parts[1];
		int age1=Integer.parseInt(token[1].trim());
		//String age12=String.format("%d", age1);
		//String Education1= age12+ "," +Education;
	if(age1>=(18) & age1<=(25))
		{
		context.write(new Text(Education), one);
	}
}
	}
	}
	
	public static class edureducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable <IntWritable> value, Context context) throws IOException, InterruptedException{
			//int count1=0;
			//String temp=null;
			//String edu="";
			int sum=0;
			for(IntWritable val:value)
			{
				// String parts[]=value.toString().split(":");
				//temp=parts[1];
				//if(parts[1].equals("\" 6th or 7th grade"\"))
				sum=sum+val.get();
					
				}
			context.write(key,new IntWritable(sum));
			}
			
			
				
			}
		
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf =new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job=Job.getInstance(conf);
		job.setJarByClass(education.class);
		job.setJobName("education");
		job.setMapperClass(edumap.class);
		//job.addCacheFile(new Path("sample.dat").toUri());
//		job.setNumReduceTasks(0);
		job.setReducerClass(edureducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
		
	}
	
	
	
	
	
	
	
	
}

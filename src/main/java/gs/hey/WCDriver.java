package gs.hey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WCDriver {

   public static void main(String[] args) throws Exception {
	   
	   Configuration conf = new Configuration();
	         Job job = Job.getInstance(conf, "WordCounter");
	         job.setJarByClass(WCDriver.class);
	         job.setMapperClass(WCMapper.class);
	         job.setReducerClass(WCReducer.class);
	   
	         // TODO: specify output types
	         job.setOutputKeyClass(Text.class);
	         job.setOutputValueClass(Text.class);
	   
	         // TODO: specify input and output DIRECTORIES (not files)
	     //    FileInputFormat.setInputPaths(job, new Path("/sms1.csv"));
	      //   FileOutputFormat.setOutputPath(job, new Path("/temp/"));
	         FileInputFormat.setInputPaths(job, new Path(args[0]));
	   
	         FileOutputFormat.setOutputPath(job, new Path(args[1]));
	   
	         if (!job.waitForCompletion(true))
	            return;
	      }
   }






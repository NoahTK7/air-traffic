// cc GetSegments Application to find the maximum temperature in the weather dataset
// vv GetSegments
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GetSegments {

  public static void main(String[] args) throws Exception {
    // TODO: get number of decimals to round to from args and make configuration object
    if (args.length != 2) {
      System.err.println("Usage: GetSegments <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(GetSegments.class);
    job.setJobName("Air Traffic");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(GetSegmentsMapper.class);
    job.setCombinerClass(GetSegmentsReducer.class);
    job.setReducerClass(GetSegmentsReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ GetSegments

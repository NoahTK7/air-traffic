import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenSegments {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: GenSegments <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(GenSegments.class);
    job.setJobName("[air-traffic] GenSegments job");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(GenSegmentsMapper.class);
    job.setMapOutputKeyClass(FlightSnapshotKey.class);
    job.setMapOutputValueClass(FlightSnapshot.class);

    job.setReducerClass(GenSegmentsReducer.class);

    // additional classes need for secondary sorting of map output
    // adapted from https://www.oreilly.com/library/view/data-algorithms/9781491906170/ch01.html
    job.setPartitionerClass(FlightSnapshotKeyPartitioner.class);
    job.setGroupingComparatorClass(FlightSnapshotKeyGroupingComparator.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlightSegment.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

import clean.CleanMapper;
import gensegments.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ChainJob extends Configured implements Tool {
  static Configuration cf;

  public int run (String[] args)throws IOException,InterruptedException,ClassNotFoundException{
    cf = new Configuration();

    if (args.length != 2) {
      System.err.println("Usage: ChainJob <input path> <output path>");
      System.exit(-1);
    }

    Configuration mainConf = new Configuration(false);
    Job mainJob = Job.getInstance(mainConf);

    mainJob.setJarByClass(ChainJob.class);
    mainJob.setJobName("[air-traffic] main chain job");

    FileInputFormat.addInputPath(mainJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(mainJob, new Path(args[1]));

    Configuration mapAConf = new Configuration(false);
    ChainMapper.addMapper(mainJob, CleanMapper.class, LongWritable.class, Text.class,
            Text.class, Text.class, mapAConf);

    Configuration mapBConf = new Configuration(false);
    ChainMapper.addMapper(mainJob, GenSegmentsMapper.class, Text.class, Text.class,
            FlightSnapshotKey.class, FlightSnapshot.class, mapBConf);

    mainJob.setPartitionerClass(FlightSnapshotKeyPartitioner.class);
    mainJob.setGroupingComparatorClass(FlightSnapshotKeyGroupingComparator.class);

    Configuration reduceConf = new Configuration(false);
    ChainReducer.setReducer(mainJob, GenSegmentsReducer.class, LongWritable.class, Text.class,
            Text.class, Text.class,  reduceConf);

    mainJob.setOutputKeyClass(Text.class);
    mainJob.setOutputValueClass(FlightSegment.class);

    return mainJob.waitForCompletion(true)?0:1;
  }

  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new ChainJob(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

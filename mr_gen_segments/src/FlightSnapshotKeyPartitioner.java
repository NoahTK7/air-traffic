import org.apache.hadoop.mapreduce.Partitioner;

public class FlightSnapshotKeyPartitioner
        extends Partitioner<FlightSnapshotKey, FlightSnapshot> {

    @Override
    public int getPartition(FlightSnapshotKey pair,
                            FlightSnapshot snapshot,
                            int numberOfPartitions) {
        // make sure that partitions are non-negative
        return Math.abs(pair.getHex().hashCode() % numberOfPartitions);
    }
}

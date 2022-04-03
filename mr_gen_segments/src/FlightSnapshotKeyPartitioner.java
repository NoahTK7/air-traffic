import org.apache.hadoop.mapreduce.Partitioner;

public class FlightSnapshotKeyPartitioner
        extends Partitioner<FlightSnapshotKey, FlightSnapshot> {

    /**
     * This partitioner controls which keys are sent
     * to which reducer (all keys with same hex, but any timestamp).
     * This class basically tells MR which group
     * (per the GroupingComparator) a specific key belongs to.
     */
    
    @Override
    public int getPartition(FlightSnapshotKey pair,
                            FlightSnapshot snapshot,
                            int numberOfPartitions) {
        // make sure that partitions are non-negative
        return Math.abs(pair.getHex().hashCode() % numberOfPartitions);
    }
}

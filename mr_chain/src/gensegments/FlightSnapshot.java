package gensegments;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightSnapshot implements Writable {
    private final LongWritable timestamp = new LongWritable();
    private final DoubleWritable lat = new DoubleWritable();
    private final DoubleWritable lon = new DoubleWritable();
    private final Text flightRegistration = new Text();
    private final Text planeType = new Text();

    public FlightSnapshot() {
    }

    public FlightSnapshot(long timestamp, double lat, double lon, String flightRegistration, String planeType) {
        this.timestamp.set(timestamp);
        this.lat.set(lat);
        this.lon.set(lon);
        this.flightRegistration.set(flightRegistration);
        this.planeType.set(planeType);
    }

    public void set(long timestamp, double lat, double lon, String flightRegistration, String planeType) {
        this.timestamp.set(timestamp);
        this.lat.set(lat);
        this.lon.set(lon);
        this.flightRegistration.set(flightRegistration);
        this.planeType.set(planeType);
    }

    public LongWritable getTimestamp() {
        return timestamp;
    }

    public DoubleWritable getLat() {
        return lat;
    }

    public DoubleWritable getLon() {
        return lon;
    }

    public Text getFlightRegistration() {
        return flightRegistration;
    }

    public Text getPlaneType() {
        return planeType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        timestamp.write(out);
        lat.write(out);
        lon.write(out);
        flightRegistration.write(out);
        planeType.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        timestamp.readFields(in);
        lat.readFields(in);
        lon.readFields(in);
        flightRegistration.readFields(in);
        planeType.readFields(in);
    }

    @Override
    public String toString() {
        return timestamp + "," + lat + "," + lon + "," + flightRegistration + "," + planeType;
    }

}

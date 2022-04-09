package gensegments;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightSnapshot implements Writable {
    private final DoubleWritable lat = new DoubleWritable();
    private final DoubleWritable lon = new DoubleWritable();

    public FlightSnapshot() {
    }

    public FlightSnapshot(double lat, double lon) {
        this.lat.set(lat);
        this.lon.set(lon);
    }

    public void set(double lat, double lon) {
        this.lat.set(lat);
        this.lon.set(lon);
    }

    public DoubleWritable getLat() {
        return lat;
    }

    public DoubleWritable getLon() {
        return lon;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        lat.write(out);
        lon.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lat.readFields(in);
        lon.readFields(in);
    }

    @Override
    public String toString() {
        return lat + "," + lon;
    }

}

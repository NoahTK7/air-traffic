package clean;

import gensegments.FlightSegment;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RemoveStationaryMapper
        extends Mapper<Text, FlightSegment, Text, FlightSegment> {

    @Override
    public void map(Text key, FlightSegment value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] coords = line.split("\\|");
        String[] coords1 = coords[0].split(",");
        String[] coords2 = coords[1].split(",");

        String lat1 = coords1[0];
        String lon1 = coords1[1];
        String lat2 = coords2[0];
        String lon2 = coords2[1];

        if (lat1.equals(lat2) && lon1.equals(lon2)) {
            // start point is the same as end point, so don't write to output
            return;
        }

        context.write(key, value);
    }
}

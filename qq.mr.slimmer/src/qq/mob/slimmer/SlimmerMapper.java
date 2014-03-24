package qq.mob.slimmer;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SlimmerMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static enum ORIG {
		ID, ARRIVAL_TIME, DEPARTURE_TIME, ARRIVAL_TRANSPORT, DEPARTURE_TRANSPORT, ARRIVAL_ZCTA, DEPARTURE_ZCTA, ARRIVAL_LOCALE, DEPARTURE_LOCALE, ARRIVAL_LAT, ARRIVAL_LON, DEPARTURE_LAT, DEPARTURE_LON, ARRIVAL_AIRPORT_CODE, DEPARTURE_AIRPORT_CODE, ARRIVAL_MODE, DEPARTURE_MODE
	};

	@Override
	protected void map(LongWritable key, Text value, Context ctx)
			throws IOException, InterruptedException {
		String[] ss = value.toString().split("\t");
		String id = ss[ORIG.ID.ordinal()];

		String[] rest = new String[6];
		rest[0] = ss[ORIG.ARRIVAL_TIME.ordinal()];
		rest[1] = ss[ORIG.DEPARTURE_TIME.ordinal()];
		rest[2] = ss[ORIG.ARRIVAL_LOCALE.ordinal()];
		rest[3] = ss[ORIG.DEPARTURE_LOCALE.ordinal()];
		rest[4] = ss[ORIG.ARRIVAL_ZCTA.ordinal()];
		rest[5] = ss[ORIG.DEPARTURE_ZCTA.ordinal()];
		String output = Arrays.toString(rest);
		ctx.write(new Text(id), new Text(output));
	}
}
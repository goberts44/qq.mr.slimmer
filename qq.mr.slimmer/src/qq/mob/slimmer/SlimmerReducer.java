package qq.mob.slimmer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SlimmerReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx)
			throws IOException, InterruptedException {
		for (Text value : values) {
			ctx.write(key, value);
		}
	}
}
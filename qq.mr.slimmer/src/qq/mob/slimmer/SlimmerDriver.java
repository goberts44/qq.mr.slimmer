package qq.mob.slimmer;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlimmerDriver implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(SlimmerDriver.class);

	@Option(name = "-i", aliases = "--input", required = false, usage = "")
	private String input = "";

	@Option(name = "-o", aliases = "--output", required = false, usage = "")
	private String output = "";
	
	@Option(name = "-w", aliases = "--write", required = false, usage = "")
	private boolean overwrite;

	public SlimmerDriver(String[] args) throws CmdLineException {
		super();
		CmdLineParser CLI = new CmdLineParser(this);
		try {
			CLI.parseArgument(args);
		} catch (CmdLineException e) {
			CLI.printUsage(System.out);
			throw e;
		}
		log.info(this.getClass().getName() + "==>");
	}

	@SuppressWarnings("deprecation")
	public void run() {
		Configuration conf = new Configuration();

		String scenarioText = null;

		try {
			Job job = new Job(conf);
			job.setJobName(this.getClass().getName());
			
			log.debug("scenarioText=" + scenarioText);
			FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000/"), conf);
			Path pathRoot = new Path(fs.getUri());
			
			Path pathInput = new Path(pathRoot, "/first/sample");
			log.info("pathInput=" + pathInput.toString());
			Path pathOutput = new Path(pathInput, "/first/out");
			log.info("pathOutput=" + pathOutput.toString());

			if (fs.exists(pathOutput)) {
				fs.delete(pathOutput, true);
			}
			job.setJarByClass(this.getClass());
			
			job.setMapperClass(SlimmerMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(SlimmerReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, pathInput);
			FileOutputFormat.setOutputPath(job, pathOutput);
			job.getConfiguration().set("mapred.child.java.opts", "-Xmx512m");
			job.waitForCompletion(true);
		} catch (IOException e) {
			log.error("", e.fillInStackTrace());
		} catch (NullPointerException e) {
			log.error("", e.fillInStackTrace());
		} catch (Exception e) {
			log.error("", e.fillInStackTrace());
		}
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		try {
			SlimmerDriver app = new SlimmerDriver(args);
			app.run();
		} catch (CmdLineException e) {
			log.error("", e.fillInStackTrace());
		}
		long end = System.currentTimeMillis();
		Date dateStart = new Date(start);
		Date dateEnd = new Date(end);
	}
}
package io.dataapps.chlorine.hadoop;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Scan {

	public static void main (String[] args) {
		// create Options object
		Options options = new Options();

		Option help = new Option("help", "print this message" );
		Option inputPath = Option.builder("i")
				.longOpt("input_path")
				.argName("path")
				.hasArg()
				.desc("input path to scan")
				.build();
		Option outputPath = Option.builder("o")
				.longOpt("output_path")
				.argName("path")
				.hasArg()
				.desc("Output path to store results")
				.build();
		Option queue = Option.builder("q")
				.longOpt("queue")
				.argName("name")
				.hasArg()
				.desc("job queue")
				.build();

		options.addOption(help);
		options.addOption(inputPath);
		options.addOption(outputPath);
		options.addOption(queue);

		// create the parser
		CommandLineParser parser = new DefaultParser();
		try {
			// parse the command line arguments
			CommandLine line = parser.parse( options, args );
			String strInputPath=null, strOutputPath=null, strQueue=null;
			if(line.hasOption("help")) {
				usage(options);
			}
			
			if (line.hasOption("i")) {
				strInputPath = line.getOptionValue("i");
			} else {
				usage(options);
			}
			if (line.hasOption("o")) {
				strOutputPath = line.getOptionValue("o");
			} else {
				usage(options);
			}

			if (line.hasOption("queue")) {
				strQueue = line.getOptionValue("queue");
			} 
			System.out.println ("Creating a DeepScan with the following inputs");
			System.out.println ("input-path="+strInputPath);
			System.out.println ("output-path="+strOutputPath);
			if (strQueue != null) {
				System.out.println ("queue="+strQueue);
			}
			DeepScanPipeline deep = 
					new DeepScanPipeline(strInputPath,strOutputPath,strQueue);
			deep.run();
		}
		catch( ParseException exp ) {
			// oops, something went wrong
			System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
		}
	}

	static void usage(Options options) {

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "Scan", options, true);
		System.exit(0);
	}

}

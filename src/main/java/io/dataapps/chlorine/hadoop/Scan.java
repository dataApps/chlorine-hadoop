package io.dataapps.chlorine.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

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
				.desc("Output path")
				.build();
		Option saveMatches = Option.builder("sm")
				.longOpt("save_match")
				.argName("path")
				.hasArg()
				.desc("path to save matches")
				.build();
		Option queue = Option.builder("q")
				.longOpt("queue")
				.argName("name")
				.hasArg()
				.desc("job queue")
				.build();
		Option incremental = Option.builder("inc")
				.longOpt("incremental")
				.hasArg()
				.argName("file")
				.desc("specify scan as incremental and use the timestemp in the file to determine the files to scan. " +
						"If the file is present, the timestamp will be read from the file. " +
						"If file is not present, file is automatically geneated and updated with a timestamp for subsequent scans." +
						" If both incremental and scanfrom are specified, then incremental is ignored.")
						.build();
		Option scanFrom = Option.builder("s")
				.longOpt("scanfrom")
				.argName("timeinms")
				.hasArg()
				.desc("Scan only files modified on or after the specific time. " +
						"The time is specified in milliseconds after the epoch.")
						.build();
		Option mask = Option.builder("m")
				.longOpt("mask")
				.argName("path")
				.hasArg()
				.desc("Copy the input to the specified path with sensitive values masked. The directory structure is retained.")
				.build();
		options.addOption(help);
		options.addOption(inputPath);
		options.addOption(outputPath);
		options.addOption(saveMatches);
		options.addOption(queue);
		options.addOption(incremental);
		options.addOption(scanFrom);
		options.addOption(mask);

		// create the parser
		CommandLineParser parser = new DefaultParser();
		try {
			// parse the command line arguments
			CommandLine line = parser.parse( options, args );
			String strInputPath=null, strOutputPath=null, strQueue=null;
			long scanSince = -1;
			String strIncremental= null; long scanStartTime = 0;
			String maskPath = null;
			String matchPath = null;

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
			
			if (line.hasOption("sm")) {
				matchPath = line.getOptionValue("sm");
			} 

			if (line.hasOption("q")) {
				strQueue = line.getOptionValue("q");
			} 

			if (line.hasOption("inc")) {
				strIncremental = line.getOptionValue("inc");
			} 

			if (line.hasOption("s")) {
				scanSince = Long.parseLong(line.getOptionValue("s"));
			} 
			if (line.hasOption("m")) {
				maskPath = line.getOptionValue("m");;
			} 

			System.out.println ("Creating a DeepScan with the following inputs");
			System.out.println ("input-path=" + strInputPath);
			System.out.println ("output-path=" + strOutputPath);
			if (strQueue != null) {
				System.out.println ("queue=" + strQueue);
			}
			if (strIncremental != null) {
				System.out.println ("incremental=" + strIncremental);
			}
			if (scanSince > -1) {
				System.out.println ("scanfrom=" + scanSince);
			}
			if (matchPath != null) {
				System.out.println ("save Matches at=" + matchPath);
			}
			
			if (maskPath != null) {
				System.out.println ("Mask and save masked files at=" + maskPath);
			}
			
			if (scanSince == -1 && strIncremental!=null) {
				File file = new File(strIncremental);
				scanStartTime = System.currentTimeMillis();
				if (file.exists()) {
					try (BufferedReader br = new BufferedReader(new FileReader(file))) {
						String fileContents;
						while ((fileContents = br.readLine()) != null) {
							scanSince = Long.parseLong(fileContents);
							break;
						}
					} catch (IOException e) {
					}
				}
			}
			DeepScanPipeline deep = 
					new DeepScanPipeline(strInputPath, strOutputPath, matchPath, strQueue, scanSince, maskPath);
			deep.run();
			
			//update the time stamp in file if incremental is specified.
			if (strIncremental != null) {
				File file = new File(strIncremental);
				try {
				    FileWriter f2 = new FileWriter(file, false);
				    f2.write(Long.toString(scanStartTime));
				    f2.close();
				} catch (IOException e) {
				    e.printStackTrace();
				}  
			}
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

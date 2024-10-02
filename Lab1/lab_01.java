package org.mdp.cli;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.rmi.AlreadyBoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.mdp.utils.LineAndId;
import org.mdp.utils.MemStats;



// we´ve used the 7-vgram



/**
 * Main method to run an external merge sort.
 * Batches of lines from the input are read into memory and sorted.
 * Batches are written to individual files.
 * A merge-sort is applied over the individual files, creating the final sorted output.
 * The bigger the batches (in general), the faster the sort.
 * But get too close to the Heap limit and you'll run into trouble.
 * 
 * @author Aidan
 */
public class ExternalMergeSort {
	
	public static String DEFAULT_TEMP_DIR = "tmp";
	public static String DEFAULT_TEMP_SUBDIR_PREFIX = "t";
	
	public static String BATCH_FILE_NAME_PREFIX = "batch-";
	public static String BATCH_FILE_NAME_SUFFIX = ".txt";
	public static String BATCH_FILE_GZIPPED_NAME_SUFFIX = ".gz";
	
	public static boolean GZIP_BATCHES = true;
	
	public static int TICKS = 1000000;
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, AlreadyBoundException, InstantiationException, IllegalAccessException {
		Option inO = new Option("i", "input file");
		inO.setArgs(1);
		inO.setRequired(true);
		
		Option ingzO = new Option("igz", "input file is GZipped");
		ingzO.setArgs(0);
		
		Option outO = new Option("o", "output file");
		outO.setArgs(1);
		outO.setRequired(true);
		
		Option outgzO = new Option("ogz", "output file should be GZipped");
		outgzO.setArgs(0);
		
		Option bO = new Option("b", "size of batches to use");
		bO.setArgs(1);
		bO.setRequired(true);
		
		Option rO = new Option("r", "reverse (descending) order");
		rO.setArgs(0);
		
		Option kO = new Option("k", "print first k lines to std out when finished");
		kO.setArgs(1);
		kO.setRequired(false);
		
		Option tmpO = new Option("tmp", "temporary folder to store batch files (default: 'tmp/')");
		tmpO.setArgs(1);
		
		Option helpO = new Option("h", "print help");
				
		Options options = new Options();
		options.addOption(inO);
		options.addOption(ingzO);
		options.addOption(outO);
		options.addOption(outgzO);
		options.addOption(bO);
		options.addOption(rO);
		options.addOption(kO);
		options.addOption(tmpO);
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		// print help options and return
		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		// open the input
		String in = cmd.getOptionValue(inO.getOpt());
		boolean gzIn = cmd.hasOption(ingzO.getOpt());
		
		// open the output
		String out = cmd.getOptionValue(outO.getOpt());
		boolean gzOut = cmd.hasOption(outgzO.getOpt());
		
		// get the batch size
		int batchSize = Integer.parseInt(cmd.getOptionValue(bO.getOpt()));
		if(batchSize<=0){ 
			batchSize = Integer.MAX_VALUE;
		}
		System.err.println("Using a batch size of "+batchSize+" for the sort");
		
		// get the temporary directory
		// to store batch files in (if given)
		String tmpParent = DEFAULT_TEMP_DIR;
		if(cmd.hasOption(tmpO.getOpt())){
			tmpParent = cmd.getOptionValue(tmpO.getOpt());
		}
		
		// set the reverse flag
		boolean reverseOrder = cmd.hasOption(rO.getOpt());
		
		// if we need to print top-k afterwards
		int k = -1;
		if(cmd.hasOption(kO.getOpt())){
			k = Integer.parseInt(cmd.getOptionValue(kO.getOpt()));
		}
		
		// call the method that does the hard work
		// time it as well!
		long b4 = System.currentTimeMillis();
		externalMergeSort(in, gzIn, out, gzOut, batchSize, reverseOrder, tmpParent);
		
		
		// print first k lines of output if required
		if(k>0){
			System.err.println("Printing first "+k+" lines ...");
			System.err.flush();
			
			
			
			InputStream os = new FileInputStream(out);
			if(gzOut){
				os = new GZIPInputStream(os);
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(os,"utf-8"));
			PrintWriter pw = new PrintWriter(System.out);
			
			Head.bufferKLines(br, pw, k);
			
			br.close();
			pw.close();
		}
		
		System.err.println("\nOverall Runtime: "+(System.currentTimeMillis()-b4)/1000+" seconds");
	}
	
	public static void externalMergeSort(String in, boolean gzIn, String out, boolean gzOut, 
			int batchSize, boolean reverseOrder, String tmpFolderParent) throws IOException{
		// open a random sub-folder for batches so 
		// that two parallel sorts are unlikely to overwrite
		// each other
		String tmpFolder = createRandomFreshSubdir(tmpFolderParent);
		
		// open the input
		InputStream is = new FileInputStream(in);
		if(gzIn){
			is = new GZIPInputStream(is);
		}
		BufferedReader input = new BufferedReader(new InputStreamReader(is,"utf-8"));
		System.err.println("Reading from "+in);
		
		// open the output
		OutputStream os = new FileOutputStream(out);
		if(gzOut){
			os = new GZIPOutputStream(os);
		}
		PrintWriter output = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(os),"utf-8"));
		System.err.println("Writing to "+out+"\n");
		
		// batch the data into small sorted files and
		// return the batch file names
		long b4 = System.currentTimeMillis();
		ArrayList<String> batches = writeSortedBatches(input, tmpFolder, batchSize, reverseOrder);
		System.err.println("Batch Runtime: "+(System.currentTimeMillis()-b4)/1000+" seconds");
		input.close();
		
		// merge-sort the batches into the output file
		b4 = System.currentTimeMillis();
		mergeSortedBatches(batches, output, reverseOrder);
		System.err.println("Merge Runtime: "+(System.currentTimeMillis()-b4)/1000+" seconds");
		output.close();
	}
	
	/**
	 * Break the input into small sorted files containing
	 * a maximum of batchSize lines each.
	 * 
	 * @param in Reader over input file
	 * @param tmpFolder A folder in which batches can be written
	 * @param batchSize Maximum size for a batch
	 * @param reverseOrder If sorting should be in descending order
	 * @return
	 * @throws IOException
	 */
	private static ArrayList<String> writeSortedBatches(BufferedReader in,
			String tmpFolder, int batchSize, boolean reverseOrder) throws IOException {
		// this stores the file names of the batches produced ...
		ArrayList<String> batchNames = new ArrayList<String>();
		int batchId = 0;

		// this stores the lines of the file for sorting
		ArrayList<String> lines = new ArrayList<String>(batchSize);
		
		boolean done = false;
		while(!done){
			String line = in.readLine();
			if(line!=null){
				lines.add(line);
			} else {
				done = true;
			}
			
			// if the batch is full or its the last line
			// of the input, write the batch to file
			if(lines.size()==batchSize || (done && !lines.isEmpty())){
				batchId ++;
				
				// if reverse order is set, then reverse the order
				if(reverseOrder){
					Collections.sort(lines, Collections.reverseOrder());
				} else{
					Collections.sort(lines);
				}
				
				// we will return the names of the batch files later
				batchNames.add(writeBatch(lines, tmpFolder, batchId));
				lines.clear();
			}
		}
		
		return batchNames;
	}
	
	/**
	 * Opens a batch file and writes all the lines to it.
	 * @param lines
	 * @param tmpFolder
	 * @param batchId
	 * @return The filename of the batch.
	 * @throws IOException
	 */
	private static String writeBatch(Collection<String> lines, String tmpFolder, int batchId) throws IOException{
		String batchFileName = getBatchFileName(tmpFolder, batchId);
		
		System.err.println("Opening batch at "+batchFileName+" to write "+lines.size()+" lines");
		System.err.println(MemStats.getMemStats());
		PrintWriter batch = openBatchFileForWriting(batchFileName);
		
		for(String l:lines)
			batch.println(l);
		
		batch.close();
		System.err.println("... closing batch.\n");
		return batchFileName;
	}
	
	/**
	 * Merge sorted batches into one file.
	 * 
	 * @param batches The filenames of the batches to merge
	 * @param out The output to write the merged data
	 * @param reverseOrder If the ordering should be descending
	 * @throws IOException
	 */
	private static void mergeSortedBatches(ArrayList<String> batches,
			PrintWriter out, boolean reverseOrder) throws IOException {
		// inputs for all the batches
		BufferedReader[] batchReaders = new BufferedReader[batches.size()];
		// open each batch
		for(int i=0; i<batchReaders.length; i++){
			batchReaders[i] = openBatchFileForReading(batches.get(i));
		}
		
		//TODO implement merge sort on the lines of the file
		// INPUT: readers for input files given as batchReaders[]
		// OUTPUT: void (write sorted lines to out)
		// NOTE: sorted order should be reversed if reverseOrder is true.
		
		// Create a TreeSet to keep lines sorted
	    TreeSet<LineAndId> sortBuffer;
	    if (reverseOrder) {
	        sortBuffer = new TreeSet<>(Collections.reverseOrder());
	    } else {
	        sortBuffer = new TreeSet<>();
	    }
	    
	    // Read the first line from each batch and add to sortBuffer
	    for (int i = 0; i < batchReaders.length; i++) {
	        String line = batchReaders[i].readLine();
	        if (line != null) {
	            sortBuffer.add(new LineAndId(line, i));
	        }
	    }
	    
	    // Merge the batches
	    while (!sortBuffer.isEmpty()) {
	        // Get the lowest line from sortBuffer
	        LineAndId lowest = sortBuffer.pollFirst();
	        out.println(lowest.getString());
	        
	        // Read the next line from the same batch and add to sortBuffer
	        String nextLine = batchReaders[lowest.getNumber()].readLine();
	        if (nextLine != null) {
	            sortBuffer.add(new LineAndId(nextLine, lowest.getNumber()));
	        }
	    }
	}


	/**
	 * Get a batch file name with the given directory and batch number
	 * 
	 * @param dir
	 * @param batchNumber
	 * @return
	 */
	private static String getBatchFileName(String dir, int batchNumber){
		String fileName = dir+"/"+BATCH_FILE_NAME_PREFIX+batchNumber+BATCH_FILE_NAME_SUFFIX;
		if(GZIP_BATCHES)
			fileName = fileName+BATCH_FILE_GZIPPED_NAME_SUFFIX;
		return fileName;
	}
	
	/**
	 * Opens a PrintWriter for the batch filename
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	private static PrintWriter openBatchFileForWriting(String fileName) throws IOException{
		OutputStream os = new FileOutputStream(fileName);
		if(GZIP_BATCHES){
			os = new GZIPOutputStream(os);
		}
		return new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(os),"utf-8"));
	}
	
	/**
	 * Opens a BufferedReader to read from a batch.
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	private static BufferedReader openBatchFileForReading(String fileName) throws IOException{
		InputStream os = new FileInputStream(fileName);
		if(GZIP_BATCHES){
			os = new GZIPInputStream(os);
		}
		return new BufferedReader(new InputStreamReader(os,"utf-8"));
	}

	/**
	 * Creates a random sub-directory that doesn't already exist
	 * 
	 * Makes sure different runs don't overwrite each other
	 * 
	 * @param inDir Parent directory
	 * @return
	 */
	public static final String createRandomFreshSubdir(String inDir){
		boolean done = false;
		String subDir = null;
		
		while(!done){
			Random r = new Random();
			int rand = Math.abs(r.nextInt());
			subDir = inDir+"/"+DEFAULT_TEMP_SUBDIR_PREFIX+rand+"/";
			File subDirF = new File(subDir);
			if(!subDirF.exists()){
				subDirF.mkdirs();
				done = true;
			}
		}
		return subDir;
	}
}
package org.queens;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;


public class Map extends Mapper<LongWritable, Text, Text, ArrayPrimitiveWritable> {

	
	
	enum Gauge{POSITIVE, NEGATIVE}

	private Set<String> posWords = new HashSet<String>();
	private Set<String> negWords = new HashSet<String>();
	
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    private static final Pattern CON_BOUNDARY=Pattern.compile("\\t");
	protected void 
		setup(Mapper<LongWritable, Text, Text, ArrayPrimitiveWritable>.Context context)
		throws IOException, InterruptedException
	{
		if (context.getInputSplit() instanceof FileSplit)
		{
			((FileSplit) context.getInputSplit()).getPath().toString();
		} else {
			context.getInputSplit().toString();
		}
		
		URI[] localPaths = context.getCacheFiles();
		
		int uriCount = 0;
		
		parsePositive(localPaths[uriCount++]);
		parseNegative(localPaths[uriCount]);
	}
	

	private void parsePositive(URI posWordsUri) {
		try {
			@SuppressWarnings("resource")
			BufferedReader fis = new BufferedReader(new FileReader(
					new File(posWordsUri.getPath()).getName()));
			String posWord;
			while ((posWord = fis.readLine()) != null) {
				posWords.add(posWord);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception parsing cached file '"
					+ posWords + "' : " + StringUtils.stringifyException(ioe));
		}
	}
  
	private void parseNegative(URI negWordsUri) {
		try {
			@SuppressWarnings("resource")
			BufferedReader fis = new BufferedReader(new FileReader(
					new File(negWordsUri.getPath()).getName()));
			String negWord;
			while ((negWord = fis.readLine()) != null) {
				negWords.add(negWord);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing cached file '"
					+ negWords + "' : " + StringUtils.stringifyException(ioe));
		}
	}
  	@Override
	public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
		String line = lineText.toString();
		line = line.toLowerCase();
		String[] temp=CON_BOUNDARY.split(line);
		String productId=temp[1];
		if(temp.length>=8) {
			int pos=0,neg=0;
			for (String word : WORD_BOUNDARY.split(temp[7]))
			{
				if (word.isEmpty()) {
					continue;
				}
				if (posWords.contains(word)) {
					pos++;
				}
				if (negWords.contains(word)) {
					neg++;
				}
			}
			if(pos>neg)
				context.write(new Text( productId), toArray(1, 0) );
			if(pos<neg)
				context.write(new Text( productId), toArray(0, 1) );
		}
	}
  	private ArrayPrimitiveWritable toArray(int v1, int v2){     
        return new ArrayPrimitiveWritable( new int[]{v1, v2} );
    }   
}

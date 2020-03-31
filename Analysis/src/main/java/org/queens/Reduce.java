package org.queens;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends 
	Reducer<Text, ArrayPrimitiveWritable, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context)
			throws IOException, InterruptedException{
		Iterator<ArrayPrimitiveWritable> i = values.iterator();
	      int pos= 0, neg = 0;
	      while ( i.hasNext() ){
	          int[] counts = (int[])i.next().get();
	          pos += counts[0];
	          neg += counts[1];
	      }
	      context.write( key, new Text((pos+neg)+ "    " + (float)pos/(pos+neg)*100));
	}
}

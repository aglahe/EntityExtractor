package com.datatactics.memex.extractor.accumulo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class AccumuloSinkReducer extends Reducer<BytesWritable, MapWritable, Text, Mutation>
{
	private static final Logger log = Logger.getLogger(AccumuloSinkReducer.class);

	@Override
	protected void setup(Context context)
	{
		Configuration conf = context.getConfiguration();
	}
	
	@Override
	protected void reduce(BytesWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException 
	{
	}
}

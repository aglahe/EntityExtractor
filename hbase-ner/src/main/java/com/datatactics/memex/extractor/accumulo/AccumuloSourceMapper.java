package com.datatactics.memex.extractor.accumulo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AccumuloSourceMapper extends Mapper<LongWritable, Text, BytesWritable, MapWritable>
{
	@Override
	protected void setup(Context context)
	{
		Configuration conf = context.getConfiguration();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
	{
	}
}

package com.datatactics.memex.extractor.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.datatactics.memex.extractor.hadoop.TextArrayWritable;

public class HbaseExtractorReducer extends TableReducer<BytesWritable, MapWritable, Text> 
{
    private static final Logger log = Logger.getLogger(HbaseExtractorReducer.class);

	public static final byte[] SNER_CF = "sner".getBytes();
	public static final byte[] JSON_CQ = "json".getBytes();

	public static final byte[] TABLE = "aglahe-test-ee".getBytes();

	private HBaseAdmin admin;
	
    @Override
    protected void setup(Context context)
    {
        log.info("Setup HBaseReducer");
    }

    @Override
    protected void reduce(BytesWritable rowId, Iterable<MapWritable> values, Context context) throws IOException
    {
		Put put = new Put(rowId.getBytes());
		for (MapWritable val : values) 
		{
			for (Map.Entry<Writable, Writable> entry : val.entrySet())
			{
				String colFamily = ((Text)entry.getKey()).toString();
				byte[] colFamilyBytes = colFamily.getBytes();
				
				// Need to see if this col. family exists
				if (this.admin.tableExists(TABLE))
				{
					log.info("Found table");
					HTableDescriptor tableDes = this.admin.getTableDescriptor(TABLE);
					if (tableDes.hasFamily(colFamilyBytes))
					{
						log.info("Family: " + colFamily + " exists");
						TextArrayWritable textTokenArray = ((TextArrayWritable)entry.getValue());
						List<Text> textTokens = textTokenArray.getTexts();
						for (Text txt : textTokens)
						{
							byte[] colQualiferBytes = txt.getBytes();
							log.info("Add CQ: " + new String(colQualiferBytes));
							put.add(colFamilyBytes, colQualiferBytes, null);
						}
						
						try 
						{
							context.write(null, put);
						}
						catch (InterruptedException e) 
						{
							log.error("Error writing to HBase", e);
						}
					}
					else
					{
						log.info("Family: " + colFamily  +" does not exist");
					}
				}
				else
				{
					log.warn(TABLE + " does not exists");
				}
			}
		}
    }
}

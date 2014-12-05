package com.datatactics.memex.extractor;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.datatactics.memex.extractor.accumulo.AccumuloSinkReducer;
import com.datatactics.memex.extractor.accumulo.AccumuloSourceMapper;
import com.datatactics.memex.extractor.hbase.HbaseExtractorMapper;
import com.datatactics.memex.extractor.hbase.HbaseExtractorReducer;
import com.datatactics.memex.extractor.util.SinkSourceType;

public class ExtractorDriver extends Configured implements Tool
{
    private static final Logger log = Logger.getLogger(ExtractorDriver.class);

    @Override
    public int run(String[] args) throws Exception
    {
        // Calendar/Time to be used in a few places
        final Calendar cal = Calendar.getInstance();
        final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy HH:mm");

        // Construct the Job name, using Date to help easily find in JT
        StringBuilder jobName = new StringBuilder("Text Enrichment: ");
        jobName.append(dateFormat.format(cal.getTime()));

        Configuration conf = getConf();

        try
        {
        	SinkSourceType source = SinkSourceType.fromString(conf.get("source.type"));
        	SinkSourceType sink = SinkSourceType.fromString(conf.get("sink.type"));

        	// Make the HBase Conf for use if either 
//        	if ((source == BigTableType.HBASE) || (sink == BigTableType.HBASE))
//        	{
//                conf = HBaseConfiguration.create(getConf());
//                log.info("zookeepers = " + conf.get("hbase.zookeeper.quorum"));
//                log.info("rootdir = " + conf.get("hbase.rootdir"));
//        	}

        	Job job = Job.getInstance(conf, jobName.toString());
            job.setJarByClass(this.getClass());

        	// Create the input/output formats
            setupSource(job, conf, source);
            setupSink(job, conf, sink);

            return job.waitForCompletion(true) ? 0 : 1;

        }
        catch (IllegalArgumentException e)
        {
        	log.error("Could not determine repo type", e);
        }
        
        // Something wicked this way comes...
        return 1;
    }
    
    private void setupSource(Job job, Configuration conf, SinkSourceType type)  throws IOException, AccumuloSecurityException
    {
        String sourceTableName = conf.get("table.source");
        log.info("Source Table Name = " + sourceTableName);
        
    	if (type == SinkSourceType.HBASE)
    	{
    		log.info("HBase Source Chosen");

    		Scan scan = new Scan();
            scan.addFamily("orig".getBytes());
            scan.setCacheBlocks(false);

            // Init the input/output formats
			TableMapReduceUtil.initTableMapperJob(sourceTableName, scan, 
					HbaseExtractorMapper.class, BytesWritable.class, MapWritable.class, job);
    	}
    	else
    	{
    		log.info("Accumulo Source Chosen");
			job.setMapperClass(AccumuloSourceMapper.class);
			job.setMapOutputKeyClass(BytesWritable.class);
			job.setMapOutputValueClass(MapWritable.class);
			
			AccumuloInputFormat.setConnectorInfo(job, conf.get("accumulo.user"), new PasswordToken(conf.get("accumulo.password")));
			AccumuloInputFormat.setZooKeeperInstance(job, conf.get("accumulo.instance"), conf.get("accumulo.zookeepers"));
			AccumuloInputFormat.setScanAuthorizations(job, new Authorizations());
			AccumuloInputFormat.setInputTableName(job, sourceTableName);
			job.setInputFormatClass(AccumuloInputFormat.class);
    	}
    }
    
    private void setupSink(Job job, Configuration conf, SinkSourceType type) throws IOException, AccumuloSecurityException
    {
        String sinkTableName = conf.get("table.sink");
        log.info("Sink Table Name = " + sinkTableName);

    	if (type == SinkSourceType.HBASE)
    	{
    		log.info("HBase Sink Chosen");
            TableMapReduceUtil.initTableReducerJob(sinkTableName, HbaseExtractorReducer.class, job);
    	}
    	else
    	{
    		log.info("Accumulo Sink Chosen");
    		
			job.setReducerClass(AccumuloSinkReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Mutation.class);

			AccumuloOutputFormat.setConnectorInfo(job, conf.get("accumulo.user"), new PasswordToken(conf.get("accumulo.password")));
    		AccumuloOutputFormat.setZooKeeperInstance(job, conf.get("accumulo.instance"), conf.get("accumulo.zookeepers"));
    		AccumuloOutputFormat.setCreateTables(job, true);
    		AccumuloOutputFormat.setDefaultTableName(job, sinkTableName);
			job.setOutputFormatClass(AccumuloOutputFormat.class);
    	}
    }

    public static void main(String[] args) throws Exception
    {
        int exitCode = ToolRunner.run(new Configuration(), new ExtractorDriver(), args);
        System.exit(exitCode);
    }
}

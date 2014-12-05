package com.datatactics.memex.extractor.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import com.datatactics.memex.extractor.hadoop.MultiMapTransformer;
import com.datatactics.memex.extractor.ner.NER;
import com.google.common.collect.LinkedHashMultimap;
import com.gravity.goose.Article;
import com.gravity.goose.Goose;

public class HbaseExtractorMapper extends TableMapper<BytesWritable, MapWritable> 
{
    private static final Logger log = Logger.getLogger(HbaseExtractorMapper.class);

    public static final byte[] ORIG_CF = "orig".getBytes();
	public static final byte[] TEXT_CQ = "cleaned_text".getBytes();

    private NER ner;
    private MultiMapTransformer transformer;

    private boolean useGoose;
    private Goose goose;
    
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        log.info("Setup ExtractorMapper");
		Configuration conf = context.getConfiguration();

        this.ner = new NER();
        
        // Transform to Hadoop friendly types
        this.transformer = new MultiMapTransformer();
        
        this.useGoose = conf.getBoolean("usegoose", false);
        if (this.useGoose)
        {
        	this.goose = new Goose(new com.gravity.goose.Configuration());
        }
    }

    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException
    {
    	if (value.containsColumn(ORIG_CF, TEXT_CQ))
    	{
    		byte[] valueBytes = value.getValue(ORIG_CF, TEXT_CQ);
    		
    		String textToAnalyze;
    		if (useGoose)
    		{
    			Article article = goose.extractContent(null, new String(valueBytes));
    			textToAnalyze = article.cleanedArticleText();
    		}
    		else 
    		{
        		textToAnalyze = new String(valueBytes);
    		}
    		
			// Process the text
    		LinkedHashMultimap<String, String> processedText = ner.process(textToAnalyze);
    		
    		// The Map we'll send on to the reducer
    		MapWritable map = transformer.transform(processedText);
    		
    		try 
        	{
    			context.write(new BytesWritable(key.copyBytes()), map);
    		} 
        	catch (InterruptedException e) 
        	{
    			log.error("Error writing Mapper output", e);
    		}
    	}
    	else
    	{
    		log.info("Result did not contain ORIG CF and/or Cleaned text CQ");
    	}
    }
}

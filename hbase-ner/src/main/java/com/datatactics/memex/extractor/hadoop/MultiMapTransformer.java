package com.datatactics.memex.extractor.hadoop;

import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.LinkedHashMultimap;

public class MultiMapTransformer 
{
    private static final Logger log = Logger.getLogger(MultiMapTransformer.class);
	
	public MapWritable transform(LinkedHashMultimap<String, String> processedText)
	{
		// The Map we'll send on to the reducer
		MapWritable map = new MapWritable();
		
		Set<String> nerTerms = processedText.keySet();
		for (String nerTerm : nerTerms)
		{
			Set<String> textTokens = processedText.get(nerTerm);
			if ((textTokens != null) && !textTokens.isEmpty())
			{
				log.info("nerTerm = " + nerTerm);

				// Only add things worth adding :)
				if (!nerTerm.equals("O"))
				{
    				String[] textTokensArray = textTokens.toArray(new String[textTokens.size()]);
    				log.info("text Array = " + Arrays.toString(textTokensArray));
    				
    				// Put into the Map
    				map.put(new Text(nerTerm), new TextArrayWritable(textTokensArray));
				}
				else
				{
					log.info("nerTerm = O");
				}
			}
		}

		return map;
	}
}

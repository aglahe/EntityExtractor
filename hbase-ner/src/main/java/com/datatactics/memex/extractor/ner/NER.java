package com.datatactics.memex.extractor.ner;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.PeekingIterator;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class NER 
{
    private static final Logger log = Logger.getLogger(NER.class);
	public static final String JSON_KEY = "json";

	private StanfordCoreNLP pipeline;
	
	public NER(Properties props)
	{
		this.pipeline = new StanfordCoreNLP(props);
	}
	
	public NER()
	{
	    Properties props = new Properties();
	    props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
	    
		this.pipeline = new StanfordCoreNLP(props);
	}
	
	public StanfordCoreNLP getPipeline()
	{
		return pipeline;
	}
	
	public LinkedHashMultimap<String, String> process(String text)
	{
	    Annotation document = new Annotation(text);
	    pipeline.annotate(document);

	    // Create map to return
    	LinkedHashMultimap<String, String> map = LinkedHashMultimap.create();

	    List<CoreMap> sentences = document.get(SentencesAnnotation.class);
	    for(CoreMap sentence: sentences) 
	    {
	    	List<CoreLabel> labels = sentence.get(TokensAnnotation.class);
	    	PeekingIterator<CoreLabel> it = Iterators.peekingIterator(labels.iterator());
	    	
	    	while (it.hasNext())
	    	{
	    		CoreLabel label = it.next();

	    		StrBuilder builder = new StrBuilder(label.originalText());
	    		String nerTerm = label.ner();
	    		while (it.hasNext() && it.peek().ner().endsWith(nerTerm))
	    		{
	    			String followingToken = it.next().originalText();
	    			
	    			// Add a space, if it isn't a comma
	    			if (!followingToken.equalsIgnoreCase(","))
	    			{
	    				builder.append(' ');
	    			}
	    			
	    			builder.append(followingToken);
	    		}

		        log.info(nerTerm + " : " + builder.toString());
	    		map.put(nerTerm, builder.toString());
	    	}
	    }

		return map;
	}
	
}

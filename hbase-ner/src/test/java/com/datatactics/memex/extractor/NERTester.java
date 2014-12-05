package com.datatactics.memex.extractor;

import com.datatactics.memex.extractor.ner.NER;

public class NERTester 
{
	private String text = "A woman in New York City who was being monitored for possible exposure to Ebola has died and her cause of death is being investigated by the city's Health Department";
	
	private NER ner;
	
	public NERTester ()
	{
		this.ner = new NER();
		
		ner.process(text);
	}
	
	public static void main(String[] args) 
	{
		new NERTester();
	}
}

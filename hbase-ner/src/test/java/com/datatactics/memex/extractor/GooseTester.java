package com.datatactics.memex.extractor;

import org.apache.log4j.Logger;

import com.gravity.goose.Article;
import com.gravity.goose.Goose;

public class GooseTester {

    private static final Logger log = Logger.getLogger(GooseTester.class);

    private Goose goose;

	public GooseTester()
	{
    	this.goose = new Goose(new com.gravity.goose.Configuration());

    	Article art = goose.extractContent("http://www.wwdsi.com");
    	log.info("Text = " + art.cleanedArticleText());
	}
	
	public static void main(String[] args) 
	{
		new GooseTester();
	}
}

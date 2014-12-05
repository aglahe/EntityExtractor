package com.datatactics.memex.extractor.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextArrayWritable extends ArrayWritable 
{
	public TextArrayWritable()
	{
        super(Text.class);
    }

    public TextArrayWritable(String[] strings) 
    {
    	this();
    	
    	Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) 
        {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }
    
    public List<Text> getTexts()
    {
    	List<Text> list = new ArrayList<Text>();
    	Writable[] texts = get();
    	
    	int len = texts.length;
        for (int i= 0; i<len; i++) 
        	list.add((Text)texts[i]);

    	return list;
    }
}

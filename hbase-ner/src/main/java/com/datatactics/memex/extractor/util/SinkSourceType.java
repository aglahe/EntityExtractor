package com.datatactics.memex.extractor.util;

import org.apache.commons.lang.StringUtils;

public enum SinkSourceType 
{
	HBASE("hbase"), ACCUMULO("accumulo"), HDFS("hdfs");
	
	private String t;
	
	SinkSourceType(String text)
	{
		this.t = text;
	}
	
	public static SinkSourceType fromString(String text)
	{
		if (!StringUtils.isEmpty(text))
		{
			for (SinkSourceType bge : SinkSourceType.values())
			{
				if (text.equalsIgnoreCase(bge.t))
					return bge;
			}
		}
		
		throw new IllegalArgumentException("No constanct with text: " + text + " found");
	}
	
	
}

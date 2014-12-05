package com.datatactics.memex.udf;

import java.io.IOException;
import java.util.UUID;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class UddiGenerator extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException {
		return UUID.randomUUID().toString();
	}
}
package org.pd.streaming.application;

import org.pd.streaming.aggregation.simple.IntegerSum;
import org.pd.streaming.aggregation.simple.IntegerSumWithKey;
import org.pd.streaming.aggregation.simple.IntegerSumWithKeyFromPojo;
import org.pd.streaming.aggregation.simple.IntegerSumWithReduce;

public class Main 
{
	public static void main(String[] args) throws Exception 
	{
		//IntegerSum soi = new IntegerSum();
		//IntegerSumWithReduce soi = new IntegerSumWithReduce();
		//IntegerSumWithKey soi = new IntegerSumWithKey();
		IntegerSumWithKeyFromPojo soi = new IntegerSumWithKeyFromPojo();
		soi.init();
	}
}

package org.pd.streaming.application;

import org.pd.streaming.aggregation.key.IntegerSumWithKey;
import org.pd.streaming.aggregation.key.IntegerSumWithKeyFromPojo;
import org.pd.streaming.aggregation.simple.IntegerSum;
import org.pd.streaming.aggregation.simple.IntegerSumWithReduce;

/**
 * 
 * @author preetdeep.kumar
 */
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

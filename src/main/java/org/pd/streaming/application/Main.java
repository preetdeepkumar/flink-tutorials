package org.pd.streaming.application;

import org.pd.streaming.aggregation.simple.IntegerSum;

public class Main 
{
	public static void main(String[] args) throws Exception 
	{
		IntegerSum soi = new IntegerSum();
		soi.init();
	}
}

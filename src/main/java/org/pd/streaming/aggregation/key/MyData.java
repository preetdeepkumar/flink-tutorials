package org.pd.streaming.aggregation.key;

/**
 * 
 * @author preetdeep.kumar
 *
 */
public class MyData 
{
	private String id;
	private Integer value;
	
	/**
	 * Required by Flink
	 */
	public MyData()
	{
	}
	
	public MyData(String id, Integer value)
	{
		this.id = id;
		this.value = value;
	}
	
	public String getId() 
	{
		return id;
	}
	
	public void setId(String id) 
	{
		this.id = id;
	}
	
	public Integer getValue() 
	{
		return value;
	}
	
	public void setValue(Integer value) 
	{
		this.value = value;
	}
	
	@Override
	public String toString()
	{
		return id + " : " + value;
	}
}

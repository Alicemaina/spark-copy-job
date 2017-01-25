package com.spark.copyjob;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SplitPartitions {

	public final static Long MIN_PARTITION = Long.MIN_VALUE;
	public final static Long MAX_PARTITION  = Long.MAX_VALUE;
	
	
	
	public static Collection<Partition> getSubPartitions(BigInteger splitSize, BigInteger min, BigInteger max){
		long curMax = min.longValueExact();
		long partitionSize =  max.subtract(min).divide(splitSize).longValueExact();
		List<Partition> partitions = new ArrayList<Partition>();
		if(partitionSize==0){
			partitionSize=100000;
		}
		boolean exausted = false;
		while(curMax<=max.longValueExact()){
			long curMin = curMax;
		    long newCurMax = curMin + partitionSize;
		    if (newCurMax < curMax) {
		    	newCurMax = max.longValueExact();
		    	exausted = true;
		    }
		    if(newCurMax > max.longValueExact()){
		    	newCurMax=max.longValueExact();
		    	exausted=true;
		    }
		    curMax = newCurMax;
		    partitions.add(new Partition(curMin,curMax));
		    if(exausted){
		    	break;
		    }
		}
		
		return partitions;
	}
	
	
	
	
	public static class Partition implements Serializable{
		private static final long serialVersionUID = 1L;

		private Long min;
		private Long max;
		public Partition(Long min, Long max){
			this.min = min;
			this.max = max;
		}
		
		public Long getMin() {
			return min;
		}

		public Long getMax() {
			return max;
		}

		public String toString(){
			return "min: "+ min + " max:" + max;
		}
	}
}

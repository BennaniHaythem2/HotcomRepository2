package com.log.spark.test;

import com.log.spark.LogMapReduceByKeyAddIP;
import com.log.spark.LogMapReduceKeyDate;

public class TestCount {
public static void main(String[] args) {
	LogMapReduceByKeyAddIP  countActions=new LogMapReduceByKeyAddIP();
	countActions.sCountLesActions("path.txt");
	LogMapReduceKeyDate countDate=new LogMapReduceKeyDate();
	countDate.sCountDateAddIP("path.txt");
	
}
}

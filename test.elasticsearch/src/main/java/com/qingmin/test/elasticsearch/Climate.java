package com.qingmin.test.elasticsearch;

import java.util.Date;



import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public class Climate {
	
	private String date;
	
	private String weather;
	
	private String temp;
	
	private String city;
	
	private String province;
}

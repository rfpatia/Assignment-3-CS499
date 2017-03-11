package com.javacodegeeks.examples.wordcount;

import java.util.Comparator;

public class RatingComparator implements Comparator<String[]>
{
    public int compare(String[] x, String[] y)
    {	
    	if(Integer.parseInt(y[1]) < Integer.parseInt(x[1]))
        	return -1;
        else if(Integer.parseInt(y[1]) > Integer.parseInt(x[1]))
        	return 1;
        else
        	return 0;
    }
}

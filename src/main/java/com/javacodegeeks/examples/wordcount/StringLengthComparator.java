package com.javacodegeeks.examples.wordcount;

import java.util.Comparator;

public class StringLengthComparator implements Comparator<String[]>
{
    public int compare(String[] x, String[] y)
    {
        return y[1].compareTo(x[1]);
    }
}



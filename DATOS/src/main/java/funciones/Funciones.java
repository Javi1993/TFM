package funciones;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

import org.apache.commons.lang3.StringUtils;

public class Funciones {
	
	/**
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static int getLineNumber(String path) throws IOException{
		LineNumberReader lnr = new LineNumberReader(new FileReader(new File(path)));	
		lnr.skip(Long.MAX_VALUE);
		int nlines = lnr.getLineNumber() + 1; //Add 1 because line index starts at 0
		lnr.close();
		return nlines;
	}
	
	/**
	 * 
	 * @param s1
	 * @param s2
	 * @return
	 */
	public static double similarity(String s1, String s2) {
		String longer = s1.toLowerCase(), shorter = s2.toLowerCase();
		if (s1.length() < s2.length()) { // longer should always have greater length
			longer = s2.toLowerCase(); shorter = s1.toLowerCase();
		}
		int longerLength = longer.length();
		if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
		return (longerLength - StringUtils.getLevenshteinDistance(longer, shorter)) / (double) longerLength;
	}
}

/* ByteScan
 *
 * $Id$
 *
 * Created on January 28th, 2007
 *
 * Copyright (C) 2007 Zvents
 *
 * This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 * Heritrix is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * any later version.
 *
 * Heritrix is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser Public License for more details.
 *
 * You should have received a copy of the GNU Lesser Public License
 * along with Heritrix; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.archive.io.hdfs.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * A class to parse the document objects created by the hdfs-writer-processor
 *
 * @author Doug Judd
 */
public class ByteScan {


    /**
     * Represents state of the scan of a byte array
     */
    public static class State extends ByteSeq {
	public State(ByteSeq seq) {
	    super.init(seq.buf, seq.offset, seq.end);
	}
	public State() {
	    super.init(null, 0, 0);
	}
	public State(byte [] buf) {
	    super.init(buf, 0, buf.length);
	}
	public State(byte [] buf, int offset, int end) {
	    super.init(buf, offset, end);
	}
	public void init(byte [] buf, int offset, int end) {
	    super.init(buf, offset, end);
	}
	public boolean eob() { return (offset >= end); }
	public void mark() { mark = offset; }
	public void reset() { offset = mark; }
	public void flip() { int tmp = offset; offset = mark; mark = tmp; }
	public void caseSensitive(boolean cs) { caseSensitive = cs; }

	public String toString() { return new String(buf, mark, offset-mark); }
	
	public int mark;
	public int ival;
    }


    /**
     * Skips whitespace
     *
     * @param bss byte array scan state
     * @return false if end of array, true otherwise
     */
    public static boolean SkipWhitespace(State bss) {
	while (bss.offset < bss.end &&
	       Character.isWhitespace(bss.buf[bss.offset]))
	    bss.offset++;
	return (bss.offset == bss.end) ? false : true;
    }


    /**
     * Skips to whitespace
     *
     * @param bss byte array scan state
     * @return false if end of array, true otherwise
     */
    public static boolean SkipToWhitespace(State bss) {
	while (bss.offset < bss.end &&
	       !Character.isWhitespace(bss.buf[bss.offset]))
	    bss.offset++;
	return (bss.offset == bss.end) ? false : true;
    }


    /**
     * Parse integer
     *
     * @param bss byte array scan state
     * @return false if integer format error, true otherwise
     */
    public static boolean ParseInt(State bss) {
	int base = bss.offset;
	while (bss.offset < bss.end &&
	       Character.isDigit(bss.buf[bss.offset]))
	    bss.offset++;
	try {
	    bss.ival = Integer.parseInt(new String(bss.buf, base, bss.offset-base));
	    return true;
	}
	catch (NumberFormatException e) {
	    bss.ival = 0;
	}
	return false;
    }


    /**
     * This function is used to do a case insensitive match of a sequence
     * of ASCII encoded bytes.
     *
     * @param bss byte array scan state
     * @param str char string to match
     * @return true if current byte array positon matches string
     */
    public static boolean Equals(ByteSeq bss, char[] str) {
	if (bss.end-bss.offset != str.length && !StartsWith(bss, str))
	    return false;
	return true;
    }


    /**
     * This function is used to do a prefix match of a sequence of ASCII
     * encoded characters.  The case sensitivity of the match is determined
     * by the <code>bss.caseSensitive<code> flag.  The offset is unchanged.
     *
     * @param bss input byte array
     * @param str char string to match
     * @return true if current byte array positon starts with string
     */
    public static boolean StartsWith(ByteSeq bss, char[] str) {

	if (bss.end-bss.offset < str.length)
	    return false;

	for (int i=0; i<str.length; i++) {
	    if (bss.caseSensitive) {
		if (bss.buf[bss.offset+i] != str[i])
		    return false;
	    }
	    else if (Character.toLowerCase(bss.buf[bss.offset+i]) != str[i] &&
		     Character.toUpperCase(bss.buf[bss.offset+i]) != str[i])
		return false;
	}
	return true;
    }


    /**
     * This function is used to do a  prefix match of a sequence ASCII
     * encoded characters.  The case sensitivity of the match is determined
     * by the <code>bss.caseSensitive<code> flag.  If there is a match, then
     * the offset will be set to the first character after the match,
     * otherwise the offset is unchanged.
     *
     * @param bss input byte array
     * @param str char string to match
     * @return true if current byte array positon starts with string
     */
    public static boolean StartsWithSkip(State bss, char[] str) {
	if (StartsWith(bss, str)) {
	    bss.offset += str.length;
	    return true;
	}
	return false;
    }


    /**
     * This function is used to find the offset of a given character
     * in the supplied byte array.  If the character is found then
     * the offset will be set to the offset of the character in the
     * buffer, otherwise the offset will equal the end offset.
     *
     * @param bss input byte array
     * @param c character to search for
     * @return true if character found, false otherwise
     */
    public static boolean Find(State bss, char c) {
	while(bss.offset<bss.end && bss.buf[bss.offset] != c)
	    bss.offset++;
	if (bss.offset == bss.end)
	    return false;
	return true;
    }


    /**
     * This function is used to find the offset of a given character
     * in the supplied byte array.  If the character is found then
     * the offset will be set to the character immediately following
     * the found character, otherwise the offset will equal the end
     * offset.
     *
     * @param bss input byte array
     * @param c character to search for
     * @return true if character found, false otherwise
     */
    public static boolean FindSkip(State bss, char c) {
	if (Find(bss, c)) {
	    bss.offset++;
	    return true;
	}
	return false;
    }

    /**
     * This function is used to find the offset of a given sequence of
     * ASCII encoded characters.  Case sensitivity is determined by the
     * <code>bss.caseSensitive</code> flag.  If the sequence is found
     * then the offset will be set to the first character of the sequence
     * in the buffer, otherwise the offset will equal the end offset.
     *
     * @param bss input byte array
     * @param str character string to match
     * @return true if char sequence(s) found, false otherwise
     */
    public static boolean Find(State bss, char[] str) {
	int i;
	int endOffset = bss.end - (str.length-1);
	while(bss.offset < endOffset) {
	    if (bss.caseSensitive) {
		while (bss.offset < endOffset && bss.buf[bss.offset] != str[0])
		    bss.offset++;
	    }
	    else {
		while (bss.offset < endOffset &&
		       Character.toLowerCase(bss.buf[bss.offset]) != str[0] &&
		       Character.toUpperCase(bss.buf[bss.offset]) != str[0])
		    bss.offset++;
	    }
	    if (bss.offset < endOffset) {
		if (bss.caseSensitive) {
		    for (i=1; i<str.length; i++) {
			if (bss.buf[bss.offset+i] != str[i])
			    break;
		    }
		}
		else {
		    for (i=1; i<str.length; i++) {
			if (Character.toLowerCase(bss.buf[bss.offset+i]) != str[i] &&
			    Character.toUpperCase(bss.buf[bss.offset+i]) != str[i])
			    break;
		    }
		}
		if (i==str.length)
		    return true;
		bss.offset++;
	    }
	}
	return false;
    }


    /**
     * This function is used to find the offset of a given sequence of
     * ASCII encoded characters.  Case sensitivity is determined by the
     * <code>bss.caseSensitive</code> flag.  If the sequence is found
     * then the offset will be set to the position after the match in
     * the buffer, otherwise the offset will equal the end offset.
     *
     * @param bss input byte array
     * @param str character string to match
     * @return true if char sequence(s) found, false otherwise
     */
    public static boolean FindSkip(State bss, char[] str) {
	if (Find(bss, str)) {
	    bss.offset += str.length;
	    return true;
	}
	return false;
    }


    /**
     * Returns the contents of the file in a byte array.
     * 
     * @param file File to get bytes from
     * @return byte array holding file contents
     */
    public static byte[] getBytesFromFile(File file) throws IOException {
	InputStream is = new FileInputStream(file);
    
	// Get the size of the file
	long length = file.length();
    
	// You cannot create an array using a long type.
	// It needs to be an int type.
	// Before converting to an int type, check
	// to ensure that file is not larger than Integer.MAX_VALUE.
	if (length > Integer.MAX_VALUE) {
	    // File is too large
	}
    
	// Create the byte array to hold the data
	byte[] bytes = new byte[(int)length];
    
	// Read in the bytes
	int offset = 0;
	int numRead = 0;
	while (offset < bytes.length
	       && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
	    offset += numRead;
	}
    
	// Ensure all the bytes have been read in
	if (offset < bytes.length) {
	    throw new IOException("Could not completely read file "+
				  file.getName());
	}
    
	// Close the input stream and return bytes
	is.close();
	return bytes;
    }



}


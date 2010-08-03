/* HDFSWriterDocument
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
package org.archive.io.hdfs;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import org.archive.io.hdfs.util.ByteScan;


/**
 * A class to parse and generate the document objects created by the hdfs-writer-processor
 *
 * @author Doug Judd
 */
public class HDFSWriterDocument implements Writable {

    private byte [] header =
    { 'H','D','F','S','W','r','i','t','e','r','/','0','.','2','\r','\n' };

    private char [] contentTypeChars =
    { 'c','o','n','t','e','n','t','-','t','y','p','e' };

    /*private char [] httpLower = { 'h','t','t','p' };
    private char [] httpUpper = { 'H','T','T','P' };*/

    private static int HIGH_WATER_BUFFER_LENGTH = 524288;

    protected byte [] buf = null;
    protected int pos;
    protected int length;
    private byte [] requestBase;
    private int requestOffset;
    private int requestLength;
    private byte [] responseBase;
    private int responseOffset;
    private int responseBodyOffset;
    private int responseLength;
    private int responseCode;
    private String charset;
    private String contentType;
    private HashMap<String, String> fieldMap;

    private boolean isHttp = false;
    private boolean isModified = false;

    private String url = null;
    private String scheme = null;

    private ByteScan.State bss = new ByteScan.State();
    private ByteScan.State tmpBss = new ByteScan.State();

    private String extension = null;

    /**
     * Returns the byte array holding the document
     */
    public byte [] getBytes() { 
	reconstructDocument();
	return buf;
    }

    public byte [] getRequestBytes() { return (requestBase != null) ? requestBase : buf; }
    public int getRequestOffset() { return requestOffset; }
    public int getRequestLength() { return requestLength; }

    public byte [] getResponseBytes() { return (responseBase != null) ? responseBase : buf; }
    public int getResponseOffset() { return responseOffset; }
    public int getResponseBodyOffset() { return responseBodyOffset; }
    public int getResponseLength() { return responseLength; }

    public int getResponseCode() { return responseCode; }

    public String getCharset() { return charset; }

    //private static byte [] testBytes = { 'f','o','o' };

    public String getValidCharset() {
      if (charset == null)
	return "ISO-8859-1";
      try {
	//String tmp = new String(testBytes, charset);
	return charset;
      }
      catch (Exception e) {
      }
      return "ISO-8859-1";
    }

    public String getContentType() { return contentType; }

    public String getURLScheme() { return scheme; }


    /**
     * Returns lowercased file extension
     */
    public String getURLFileExtension() { 
	if (extension != null)
	    return extension;
	if (url == null)
	    return null;
	try {
	    URL u = new URL(url);
	    String path = u.getPath();
	    int lastDot = path.lastIndexOf('.');
	    int lastSlash = path.lastIndexOf('/');
	    if (lastDot == -1 || lastDot < lastSlash)
		return null;
	    extension = path.substring(lastDot+1).toLowerCase();
	    if (extension.length() == 0)
		extension = null;
	}
	catch (MalformedURLException e) {
	    // do nothing
	}
	return extension;
    }



    /**
     * Adds a field mapping
     * 
     * @param label field label
     * @param value field value
     */
    public void setField(String label, String value) {
	if (fieldMap == null)
	    fieldMap = new HashMap<String,String>();
	fieldMap.put(label, value);
	if (label.equals("URL")) {
	    url = value;
	    int colon = url.indexOf(':');
	    if (colon > 0) {
		scheme = url.substring(0, colon).toLowerCase();
		isHttp = scheme.equals("http") ? true : false;
	    }
	}
	isModified = true;
    }

    /**
     * Get a field mapping
     * 
     * @param label field label
     * @return value for <code>label</code>
     */
    public String getField(String label) {
	return fieldMap.get(label);
    }

    /**
     * Returns a hash map of field mappings
     *
     * @return HashMap of field mappings
     */
    public HashMap<String, String> getFieldMap() { return fieldMap; }

    /**
     * Set the HTTP request
     *
     * @param httpRequest byte array holding the CRLF terminated HTTP request
     * @param offset offset within <code>httpRequest</code> of request start
     * @param length length of request
     */
    public void setHttpRequest(byte [] httpRequest, int offset, int length) {
	requestBase = httpRequest;
	requestOffset = offset;
	requestLength = length;
	isModified = true;
    }

    /**
     * Set the HTTP response
     *
     * @param httpResponse byte array holding the HTTP response
     * @param offset Offset within <code>httpResponse</code> of response start
     * @param length Length of response
     */
    public void setHttpResponse(byte [] httpResponse, int offset, int length) {
	responseBase = httpResponse;
	responseOffset = responseBodyOffset = offset;
	responseLength = length;
	parseResponse();
	isModified = true;
    }

    /**
     * Parses the given document, populating all of the interal attributes
     *
     * @param docBytes byte array holding the document
     * @param offset offset into <code>docBytes</code> where document begins
     * @param length length of document
     */
    public void load(byte [] docBytes, int offset, int length)
	throws IOException {
	int base;

	isHttp = false;
	isModified = false;

	url = null;
	scheme = null;
	extension = null;

	// allocate new buffer if necessary
	if (buf == null || buf.length < length ||
	    buf.length > HIGH_WATER_BUFFER_LENGTH) {
	    buf = new byte [length];
	}

	System.arraycopy(docBytes, offset, buf, 0, length);
	this.length = length;

	this.pos = 0;
	this.responseCode = 0;
	this.charset = null;
	this.contentType = null;

	this.requestBase = buf;
	this.requestOffset = offset;
	this.requestLength = length;

	this.responseBase = buf;
	this.responseOffset = offset;
	this.responseBodyOffset = offset;
	this.responseLength = length;

	this.url = null;
	this.scheme = null;
	this.extension = null;

	if (this.length < header.length)
	    throw new IOException("document truncated");

	for (pos=0; pos<header.length; pos++) {
	    if (buf[pos] != header[pos])
		throw new IOException("bad document header at position " + pos + "(" + (new String(buf, 0, header.length+4)) + ")");
	}

	/**
	 * Load ANVLRecord
	 */

	fieldMap = new HashMap<String,String>();

	while (pos < this.length-1) {

	    // check for ANVL termination
	    if (buf[pos] == '\n') {
		pos++;
		break;
	    }
	    else if (buf[pos] == '\r' && buf[pos+1] == '\n') {
		pos += 2;
		break;
	    }

	    base = pos;

	    // find colon
	    while (pos < this.length && buf[pos] != ':' && buf[pos] != '\n')
		pos++;

	    if (buf[pos] == '\n') {
		pos++;
		break;
	    }
	    else {
		boolean isUrl = false;
		String label = new String(buf, base, pos-base);
		if (pos-base >= 3 &&
		    buf[base]=='U' && buf[base+1]=='R' && buf[base+2]=='L')
		    isUrl = true;
		pos++;
		// skip whitespace
		while (pos < this.length && (buf[pos] == ' ' || buf[pos]=='\t'))
		    pos++;
		base = pos;
		// find LF
		while (pos < this.length && buf[pos] != '\n')
		    pos++;
		int endpos = (buf[pos-1] == '\r') ? pos-1 : pos;
		String value = new String(buf, base, endpos-base);
		// check for http
		if (isUrl) {
		    url = value;
		    int colon = url.indexOf(':');
		    if (colon > 0) {
			scheme = url.substring(0, colon).toLowerCase();
			isHttp = scheme.equals("http") ? true : false;
		    }
		}
		if (value.length() > 0)
		    fieldMap.put(label, value);
		pos++;
	    }
	}

	requestBase = buf;

	/**
	 *  Read HTTP Request
	 */
	if (isHttp) {
	    for (requestOffset = pos; pos < this.length; pos++) {
		if (buf[pos] == '\n') {
		    if (pos < this.length-2) {
			if (buf[pos+1] == '\n') {
			    requestLength = (pos+2)-requestOffset;
			    pos += 2;
			    break;
			}
			else if (buf[pos+1] == '\r' && buf[pos+2] == '\n') {
			    requestLength = (pos+3)-requestOffset;
			    pos += 3;
			    break;
			}
		    }
		    else {
			requestLength = this.length-requestOffset;
			pos = this.length;
			break;
		    }
		}
	    }
	    if (pos == this.length)
		requestLength = this.length-requestOffset;
	}
	else {
	    requestOffset = 0;
	    requestLength = 0;
	}

	/**
	 *  Parse response
	 */
	responseBase = buf;
	responseOffset = responseBodyOffset = pos;
	responseLength = this.length - responseOffset;

	if (isHttp)
	    parseResponse();
    }

    /**
     * Parses the HTTP response section, determining the body offset and
     * setting the charset field
     */
    private void parseResponse() {
	//int base;
	boolean parsingContentType = false;

	bss.init(responseBase, responseOffset, responseOffset + responseLength);

        if (!ByteScan.SkipToWhitespace(bss))
	    return;

        if (!ByteScan.SkipWhitespace(bss))
	    return;

	ByteScan.ParseInt(bss);
	responseCode = bss.ival;

	while (!bss.eob()) {

	    if (bss.buf[bss.offset] == '\n') {
		if (parsingContentType) {
		    tmpBss.init(bss.buf, bss.mark, bss.offset);
		    parseContentType(tmpBss);
		    parsingContentType = false;
		}
		if (bss.offset < bss.end-2) {
		    if (bss.buf[bss.offset+1] == '\n') {
			responseBodyOffset = bss.offset + 2;
			bss.offset += 2;
			break;
		    }
		    else if (bss.buf[bss.offset+1] == '\r' &&
			     bss.buf[bss.offset+2] == '\n') {
			responseBodyOffset = bss.offset + 3;
			bss.offset += 3;
			break;
		    }
		}
		else {
		    responseBodyOffset = bss.end;
		    bss.offset = bss.end;
		    break;
		}
		bss.offset++;
		bss.mark();
	    }
	    else if (bss.buf[bss.offset] == ':') {
		tmpBss.init(bss.buf, bss.mark, bss.offset);
		if (ByteScan.Equals(tmpBss, contentTypeChars))
		    parsingContentType = true;
		bss.offset++;
		bss.mark();
	    }
	    else
		bss.offset++;
	}

	responseBodyOffset = bss.offset;

	if (contentType == null || contentType.startsWith("text"))
	    findAndParseContentType(bss);
    }

    private char [] metaChars      = { 'm','e','t','a' };
    private char [] xmlChars       = { '?','x','m','l' };
    private char [] httpEquivChars = { 'h','t','t','p','-','e','q','u','i','v' };
    private char [] contentChars   = { 'c','o','n','t','e','n','t' };
    private char [] encodingChars  = { 'e','n','c','o','d','i','n','g' };
    private char [] closeHeadChars = { '/','h','e','a','d' };
    private char [] xmlEndChars    = { '?','>' };

    /**
     * Looks for <META http-equiv="Content-Type" ... or
     * <?xml ... encoding=" and extracts content-type and/or
     * charset info
     */
    private void findAndParseContentType(ByteScan.State bss) {

	if (!ByteScan.FindSkip(bss, '<'))
	    return;

	/**
	 * XML
	 */
	if (ByteScan.StartsWithSkip(bss, xmlChars)) {

	    // set byte scan region to that inside <?xml ... ?>
	    bss.mark();
	    if (!ByteScan.Find(bss, xmlEndChars))
		return;

	    bss.end = bss.offset;
	    bss.flip();
	    
	    // find encoding
	    if (!ByteScan.FindSkip(bss, encodingChars))
		return;

	    // find starting doublequote
 	    if (!ByteScan.FindSkip(bss, '"'))
		return;

	    bss.mark();

 	    if (!ByteScan.Find(bss, '"'))
		return;

	    charset = bss.toString().toUpperCase();

	    return;
	}

	/**
	 * HTML
	 */
	bss.offset--;
	while (ByteScan.FindSkip(bss, '<')) {

	    if (ByteScan.StartsWithSkip(bss, metaChars)) {

		// make sure next comes whitespace
		bss.mark();
		if (!ByteScan.SkipWhitespace(bss))
		    return;
		if (bss.offset == bss.mark)
		    continue;

		if (!ByteScan.StartsWithSkip(bss, httpEquivChars))
		    continue;


		if (!ByteScan.SkipWhitespace(bss))
		    return;

		// skip '=' character
		if (bss.buf[bss.offset++] != '=')
		    continue;

		if (!ByteScan.SkipWhitespace(bss))
		    return;

		// skip '"' character
		if (bss.buf[bss.offset++] != '"')
		    continue;

		bss.mark();

		if (!ByteScan.FindSkip(bss, '"'))
		    return;

		bss.flip();

		if (!ByteScan.StartsWith(bss, contentTypeChars))
		    continue;

		bss.flip();

		if (!ByteScan.SkipWhitespace(bss))
		    return;

		if (!ByteScan.StartsWithSkip(bss, contentChars))
		    return;

		if (!ByteScan.SkipWhitespace(bss))
		    return;

		// skip '=' character
		if (bss.buf[bss.offset++] != '=')
		    continue;

		if (!ByteScan.SkipWhitespace(bss))
		    return;

		// skip '=' character
		if (bss.buf[bss.offset++] != '"')
		    continue;

		bss.mark();

                if (!ByteScan.Find(bss, '"'))
		  return;

		tmpBss.init(bss.buf, bss.mark, bss.offset);

		parseContentType(tmpBss);
		if (contentType == null)
		  contentType = "text/html";
		break;
	    }
	    else if (ByteScan.Equals(bss, closeHeadChars)) {
		break;
	    }
	}
    }


    /**
     * Parses the Content-Type HTTP header
     */
    private void parseContentType(ByteScan.State bss) {

	if (!ByteScan.SkipWhitespace(bss))
	    return;

	if (bss.buf[bss.offset] == '"')
	  bss.offset++;	

	if (!ByteScan.SkipWhitespace(bss))
	    return;

	bss.mark();

	while (!bss.eob() &&
	       (Character.isLetterOrDigit(bss.buf[bss.offset]) ||
		bss.buf[bss.offset] == '/' ||
		bss.buf[bss.offset] == '-' ||
		bss.buf[bss.offset] == '+' ||
		bss.buf[bss.offset] == '.'))
	  bss.offset++;

	contentType = bss.toString().toLowerCase().trim();
	if (contentType != null && contentType.indexOf("/") == -1)
	  contentType = null;

	if (!ByteScan.Find(bss, ';'))
	    return;

	if (!bss.eob()) {
	    bss.mark();
	    bss.offset = bss.end;
	    String inputStr = bss.toString().toLowerCase();
	    String paramStr;
	    StringTokenizer st = new StringTokenizer(inputStr, ";");

	    while (st.hasMoreTokens()) {
		paramStr = st.nextToken().trim();
		if (paramStr.startsWith("charset")) {
		    int eqOff = paramStr.indexOf('=', 7);
		    if (eqOff >= 0) {
			charset = paramStr.substring(eqOff+1).trim().toUpperCase();
			charset = cleanCharset(charset);
		    }
		}
	    }
	}
    }

    private String cleanCharset(String charset) {
	if (charset.startsWith("\"") && charset.endsWith("\""))
	    charset = charset.substring(1, charset.length()-1);
        char [] csetChars = charset.toCharArray();
	int len = 0;
	while (len < csetChars.length &&
	       (Character.isLetterOrDigit(csetChars[len]) ||
		csetChars[len] == '-'))
	  len++;
	if (len < csetChars.length)
	  charset = new String(csetChars, 0, len);
	if (!charset.startsWith("ISO-") && 
	    ((charset.endsWith("8859-1") || charset.endsWith("8859_1")))) {
	    charset = "ISO-8859-1";
	}
	else if (charset.endsWith("UTF8")) {
	    charset = "UTF-8";
	}
	return charset;
    }


    /**
     * Reconstructs document, from internal fields, into byte array for serialization
     */
    private void reconstructDocument() {
	if (isModified) {
	    StringBuilder anvlBlock = new StringBuilder();
	    for (Iterator iter = fieldMap.entrySet().iterator(); iter.hasNext();) { 
		Map.Entry entry = (Map.Entry)iter.next();
		String key = (String)entry.getKey();
		String value = (String)entry.getValue();
		anvlBlock.append(key + ": " + value + "\r\n");
	    }
	    anvlBlock.append("\r\n");
	    byte [] anvlHeaderBytes = anvlBlock.toString().getBytes();
	    byte [] newbuf = new byte [ header.length + anvlHeaderBytes.length + requestLength + responseLength ];
	    int pos = 0;
	    // write header
	    System.arraycopy(header, 0, newbuf, pos, header.length);
	    pos += header.length;
	    // write ANVL fields
	    System.arraycopy(anvlHeaderBytes, 0, newbuf, pos, anvlHeaderBytes.length);
	    pos += anvlHeaderBytes.length;
	    // write request
	    if (requestLength > 0) {
		System.arraycopy(requestBase, requestOffset, newbuf, pos, requestLength);
		pos += requestLength;
	    }
	    // write response
	    System.arraycopy(responseBase, responseOffset, newbuf, pos, responseLength);
	    buf = newbuf;
	    isModified = false;
	}
    }

    /**
     * Writes the fields of this object to <code>out</code>.
     *
     * @param out output object to serialize to
     */
    public void write(DataOutput out) throws IOException {
	reconstructDocument();
	out.writeInt(buf.length);
	out.write(buf);
    }


    /**
     * Reads the fields of this object from <code>in</code>.
     *
     * @param in input object to de-serialize from
     */
    public void readFields(DataInput in) throws IOException {
	int length = in.readInt();
	byte [] docBytes = new byte [ length ];
	in.readFully(docBytes);
	load(docBytes, 0, length);
    }


    static void printUsage() {
	System.out.println("HDFSWriterDocument <input-file>");
	System.exit(1);
    }
  
    /**
     * Test driver for HDFSWriterDocument
     * @throws IOException When there is an IO error
     */
    public static void main(String[] args) throws IOException {
	HDFSWriterDocument hdfsDoc = new HDFSWriterDocument();
	HDFSWriterDocument hdfsDocNew = new HDFSWriterDocument();
	//byte [] fbytes = null;

	if (args.length < 1)
	    printUsage();

	//fbytes = ByteScan.getBytesFromFile(new File(args[0]));
	//hdfsDoc.load(fbytes, 0, fbytes.length);

	FileInputStream fis = new FileInputStream(args[0]);
	DataInputStream dis = new DataInputStream(fis);
	hdfsDoc.readFields(dis);

	System.out.println("\nANVL FIELDS:");
	HashMap<String,String> fmap = hdfsDoc.getFieldMap();
	// Assuming map = Map<String, String>
	for (Iterator iter = fmap.entrySet().iterator(); iter.hasNext();) { 
	    Map.Entry entry = (Map.Entry)iter.next();
	    String key = (String)entry.getKey();
	    String value = (String)entry.getValue();
	    System.out.println(key + ": " + value);
	    hdfsDocNew.setField(key, value);
	}
	System.out.println();

	if (hdfsDoc.getRequestLength() > 0) {
	    byte [] requestBytes = hdfsDoc.getRequestBytes();
	    String requestStr = new String(requestBytes, hdfsDoc.getRequestOffset(), hdfsDoc.getRequestLength());
	    System.out.println("REQUEST:");
	    System.out.print(requestStr);
	    hdfsDocNew.setHttpRequest(requestBytes, hdfsDoc.getRequestOffset(), hdfsDoc.getRequestLength());
	}

	if (hdfsDoc.getResponseLength() > 0) {	
	    byte [] responseBytes = hdfsDoc.getResponseBytes();
	    String responseStr = new String(responseBytes, hdfsDoc.getResponseOffset(), 512);
	    System.out.println("RESPONSE:");
	    System.out.print(responseStr);
	    System.out.print("\n[...]\n\n");
	    /*
	    if (hdfsDoc.getResponseBodyOffset() > 0) {
		responseStr = new String(responseBytes, hdfsDoc.getResponseBodyOffset(), 256);
		System.out.println("BODY:");
		System.out.print(responseStr);
		System.out.print("\n[...]\n\n");
	    }
	    */

	    hdfsDocNew.setHttpResponse(responseBytes, hdfsDoc.getResponseOffset(), hdfsDoc.getResponseLength());
	}

	System.out.println("DERIVED FIELDS:");
	System.out.println("Content-Type = '" + hdfsDoc.getContentType() + "'");
	System.out.println("Charset = '" + hdfsDoc.getCharset() + "'");
	System.out.println("Response Code = " + hdfsDoc.getResponseCode() + "'");
	System.out.println("Scheme = '" + hdfsDoc.getURLScheme() + "'");
	System.out.println("File Extension = '" + hdfsDoc.getURLFileExtension() + "'");
	System.out.println();

	if (args.length == 2) {
	    FileOutputStream fos = new FileOutputStream(args[1]);
	    DataOutputStream dos = new DataOutputStream(fos);
	    hdfsDocNew.write(dos);
	    dos.flush();
	}
    }
}

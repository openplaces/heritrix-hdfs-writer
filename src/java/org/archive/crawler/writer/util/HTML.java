/* HTML
 *
 * $Id$
 *
 * Created on February 2nd, 2007
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
package org.archive.crawler.writer.util;

import java.io.File;
import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

public class HTML {

    public static class ElementParser {

	private static char [] closeTagChars = { '<','/' };

	private char [] name;
	private ByteScan.State bss = null;
	private boolean tagOpen;
	private boolean hasContent;
	private ByteSeq dummyKey = new ByteSeq();
	private ByteSeq dummyValue = new ByteSeq();
	private ByteSeq content = new ByteSeq();

        public ElementParser() {
	    init(null, null);
	}


	public ElementParser(ByteScan.State bss, char [] name) {
	    init(bss, name);
	}

	public void init(ByteScan.State bss, char [] name) {
	    this.bss = bss;
	    tagOpen = false;
	    content.buf = null;
	    content.offset = content.end = 0;
	    this.name = name;
	}

	public void SwitchElement(char [] elementName) {
	    name = elementName;
	}

	public boolean Find() {
	    tagOpen = false;
	    hasContent = false;
	    if (bss.eob())
		return false;
	    while (ByteScan.FindSkip(bss, '<')) {
		if (ByteScan.Equals(bss, name)) {
		    bss.offset += name.length;
		    if (!bss.eob() && (Character.isWhitespace(bss.buf[bss.offset]) || bss.buf[bss.offset] == '>')) {
			tagOpen = true;
			return true;
		    }
		}
	    }
	    return false;
	}

	public void SkipAttributes() {
	    while (NextAttribute(dummyKey, dummyValue))
		;
	}

	public boolean NextAttribute(ByteSeq key, ByteSeq value) {
	    if (!tagOpen)
		return false;
	    tagOpen = false;
	    if (!ByteScan.SkipWhitespace(bss))
		return false;
	    key.buf = bss.buf;
	    value.buf = bss.buf;
	    key.offset = bss.offset;
	    if (!SkipAttrName()) {
		if (!bss.eob()) {
		    if (bss.buf[bss.offset] == '>') {
			bss.offset++;
			ParseRemaining();
		    }
		    else if (bss.buf[bss.offset] == '/') {
			bss.offset++;
			ByteScan.FindSkip(bss, '>');
		    }
		}
		return false;
	    }
	    key.end = bss.offset;
	    if (!ByteScan.SkipWhitespace(bss)) {
		return false;
	    }
	    if (bss.buf[bss.offset] != '=') {
		ByteScan.FindSkip(bss, '>');
		ParseRemaining();
		return false;
	    }
	    bss.offset++;
	    if (!ByteScan.SkipWhitespace(bss))
		return false;
	    setAttributeValue(value);
	    tagOpen = true;
	    return true;
	}

	public boolean GetContent(ByteSeq dst) {
	    if (!hasContent || content.buf == null)
		return false;
	    dst.buf = content.buf;
	    dst.offset = content.offset;
	    dst.end = content.end;
	    return true;
	}

	
	private void setAttributeValue(ByteSeq value) {
	    value.buf = bss.buf;
	    value.offset = value.end = bss.offset;
	    if (bss.eob())
		return;
	    if (bss.buf[bss.offset] == '\'' || bss.buf[bss.offset] == '"') {
		char endChar = (char)bss.buf[bss.offset++];
		value.offset = bss.offset;
		for (; !bss.eob() && bss.buf[bss.offset] != '>'; bss.offset++) {
		    if (bss.buf[bss.offset] == endChar && bss.buf[bss.offset-1] != '\\') {
			value.end = bss.offset++;
			return;
		    }
		}
		value.end = bss.offset;
		return;
	    }

	    // skip to first whitespace of '>'
	    while (!bss.eob() && !Character.isWhitespace(bss.buf[bss.offset]) &&
		   bss.buf[bss.offset] != '>')
		bss.offset++;
	    value.end = bss.offset;
	}

	private void ParseRemaining() {
	    content.buf = bss.buf;
	    content.offset = bss.offset;
	    if (FindCloseTag()) {
		content.end = bss.offset;
		// position bss to after close tag
		bss.offset += name.length + closeTagChars.length;
		if (!bss.eob() && bss.buf[bss.offset] == '>')
		    bss.offset++;
		hasContent = true;
	    }
	    else
		content.end = content.offset;
	}

	private boolean SkipAttrName() {
	    int saveOffset = bss.offset;
	    while (!bss.eob() &&
		   bss.buf[bss.offset] != '=' && bss.buf[bss.offset] != '>' && bss.buf[bss.offset] != '/' &&
		   !Character.isWhitespace(bss.buf[bss.offset]))
		bss.offset++;
	    if (bss.eob() || bss.buf[bss.offset] == '/' || bss.buf[bss.offset] == '>')
		return false;
	    return true;
	}

	private boolean FindCloseTag() {
	    int saveOffset = bss.offset;
	    while (ByteScan.FindSkip(bss, closeTagChars)) {
		if (ByteScan.Equals(bss, name)) {
		    bss.offset += name.length;
		    if (bss.eob() || bss.buf[bss.offset] == '>') {
			bss.offset -= name.length + closeTagChars.length;
			return true;
		    }
		}
	    }
	    bss.offset = saveOffset;
	    return false;
	}
	
    }

    private static char [] anchorElementData  = { 'a' };
    private static char [] headElementData    = { 'h','e','a','d' };
    private static char [] baseElementData    = { 'b','a','s','e' };
    private static char [] hrefData = { 'h','r','e','f' };
    private static char [] httpSchemeData = { 'h','t','t','p',':' };


    public static ByteSeq GetBaseHref(ByteScan.State bss) {
	ElementParser eparse = new ElementParser();
	ByteSeq attrKey = new ByteSeq();
	ByteSeq attrValue = new ByteSeq();
	ByteScan.State content = new ByteScan.State();

	eparse.init(bss, headElementData);
	if (eparse.Find()) {
	    eparse.SkipAttributes();
	    if (eparse.GetContent(content)) {
		eparse.init(content, baseElementData);
		if (eparse.Find()) {
		    while (eparse.NextAttribute(attrKey, attrValue)) {
			if (ByteScan.Equals(attrKey, hrefData))
			    return attrValue;
			break;
		    }
		}
	    }
	}
	return null;
    }


    private static String DEFAULT_URI = "http://www.zvents.com";


    /**
     * The main driver for CountCharsets map/reduce program.
     * Invoke this method to submit the map/reduce job.
     * @throws IOException When there is communication problems with the 
     *                     job tracker.
     */
    public static void main(String[] args) throws IOException {
	ByteScan.State bss;
	ElementParser eparse;
	ByteSeq attrKey = new ByteSeq();
	ByteSeq attrValue = new ByteSeq();
	ByteScan.State content = new ByteScan.State();
	URI baseURI = null;
	URI resolvedURI;
	String uriStr = DEFAULT_URI;

	if (args.length != 1) {
	    System.out.println("usage:  HTML <fname>");
	    System.exit(1);
	}

	byte [] fbytes = ByteScan.getBytesFromFile(new File(args[0]));

	bss = new ByteScan.State(fbytes);

	try {

	    eparse = new ElementParser(bss, headElementData);
	    if (eparse.Find()) {
		eparse.SkipAttributes();
		if (eparse.GetContent(content)) {
		    eparse.init(content, baseElementData);
		    if (eparse.Find()) {
			while (eparse.NextAttribute(attrKey, attrValue)) {
			    if (ByteScan.Equals(attrKey, hrefData)) {
				uriStr = new String(attrValue.buf, attrValue.offset, attrValue.end-attrValue.offset, "UTF-8");
				baseURI = new URI(uriStr);
				break;
			    }
			}
		    }
		    else
			System.out.println("No base element found");
		}
	    }
	    if (baseURI == null)
		baseURI = new URI(DEFAULT_URI);
	}
	catch (URISyntaxException e) {
	    System.err.println("URI Syntax Exception:  " + uriStr);
	    return;
	}

	bss.init(fbytes, 0, fbytes.length);
	eparse.init(bss, anchorElementData);

	//int count = 0;
	while (eparse.Find()) {
	    //System.out.println();
	    while (eparse.NextAttribute(attrKey, attrValue)) {
		if (ByteScan.Equals(attrKey, hrefData)) {
		    uriStr = new String(attrValue.buf, attrValue.offset, attrValue.end-attrValue.offset, "UTF-8");
		    try {
			if (ByteScan.StartsWith(attrValue, httpSchemeData)) {
			    resolvedURI = new URI(uriStr);
			    String path = resolvedURI.getPath();
			    if (path == null || path.length() == 0)
				resolvedURI = resolvedURI.resolve("/");
			    else
				resolvedURI.normalize();
			}
			else {
			    resolvedURI = baseURI.resolve(uriStr);
			}
			System.out.println(resolvedURI.toString());
		    }
		    catch (Exception e) {
			System.err.println("URI Syntax Exception:  " + uriStr);
		    }
		}
		//System.out.println(attrKey.toString() + " = " + attrValue.toString());
	    }
	    /*
	    if (eparse.GetContent(content)) {
		System.out.println(content.toString());
	    }
	    */
	}

    }

}

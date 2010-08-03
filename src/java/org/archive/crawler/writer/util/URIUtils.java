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

import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class URIUtils {

    private static char [] schemeEndChars = { ':', '/', '/' };
    private static char [] wwwChars = { 'w','w','w','.' };
    private static byte [] wwwBytes = { 'w','w','w','.' };

    private static char [] indexChars = { 'i','n','d','e','x','.'  };
    private static char [] defaultChars = { 'd','e','f','a','u','l','t','.' };

    private static char [] aspChars = { 'a','s','p' };
    private static char [] cfmChars = { 'c','f','m' };
    private static char [] jspChars = { 'j','s','p' };
    private static char [] htmChars = { 'h','t','m' };
    private static char [] phpChars = { 'p','h','p' };

    private static char [] htmlChars = { 'h','t','m','l' };
    private static char [] shtmChars = { 's','h','t','m' };

    private static char [] dhtmlChars = { 'd','h','t','m','l' };
    private static char [] shtmlChars = { 's','h','t','m','l' };
    private static char [] xhtmlChars = { 'x','h','t','m','l' };

    public static long ComputeHash(ByteScan.State bss, MessageDigest md) {
	int startOffset = bss.offset;
	byte [] digest;
	long lval = 0;
	boolean addSlash = false;
	int slashCount = 0;
	int authority = -1;
	int lastComp = -1;
	int query = -1;
	int fragment = -1;
	int pathEnd = -1;

	int origOffset = bss.offset;

	if (bss.offset == bss.end)
	    return 0;
	
	for (int i=bss.offset; i<bss.end; i++) {
	    if (query == -1) {
		if (bss.buf[i] == '/') {
		    if (slashCount == 1)
			authority = i+1;
		    lastComp = i+1;
		    slashCount++;
		}
		else if (bss.buf[i] == '?')
		    query = i;
	    }
	    if (bss.buf[i] == '#') {
		fragment = i;
		break;
	    }
	}

	if (authority == -1)
	    return 0;

	// strip off fragment
	if (fragment != -1)
	    bss.end = fragment;

	if (slashCount == 2) {
	    if (query != -1)
		lastComp = query;
	    else
		lastComp = bss.end;
	}

	int realPathEnd = (query == -1) ? bss.end : query;

	if (lastComp != -1) {
	    int lastCompLen = realPathEnd - lastComp;
	    bss.offset = lastComp;
	    if (ByteScan.StartsWithSkip(bss, indexChars) || 
		ByteScan.StartsWithSkip(bss, defaultChars)) {
		if (bss.end-bss.offset == 3) {
		    if (ByteScan.StartsWith(bss, aspChars) ||
			ByteScan.StartsWith(bss, cfmChars) ||
			ByteScan.StartsWith(bss, jspChars) ||
			ByteScan.StartsWith(bss, htmChars) ||
			ByteScan.StartsWith(bss, phpChars))
			pathEnd = lastComp;
		}
		else if (bss.end-bss.offset == 4) {
		    if (ByteScan.StartsWith(bss, htmlChars) ||
			ByteScan.StartsWith(bss, shtmChars))
			pathEnd = lastComp;
		}
		else if (bss.end-bss.offset == 5) {
		    if (ByteScan.StartsWith(bss, dhtmlChars) ||
			ByteScan.StartsWith(bss, shtmlChars) ||
			ByteScan.StartsWith(bss, xhtmlChars))
			pathEnd = lastComp;
		}
	    }
	}
	else
	    return 0;

	// check to see if final path slash should be added and set pathEnd
	if (pathEnd == -1) {
	    int i;
	    for (i=lastComp; i<realPathEnd; i++) {
		if (bss.buf[i] == '.')
		    break;
	    }
	    if (i == realPathEnd && bss.buf[realPathEnd-1] != '/')
		addSlash = true;
	    pathEnd = realPathEnd;
	}

	md.reset();

	// up to authority
	md.update(bss.buf, origOffset, authority-origOffset);

	// maybe add "www."
	bss.offset = authority;
	if (!ByteScan.StartsWith(bss, wwwChars))
	    md.update(wwwBytes);	    

	// to pathEnd
	md.update(bss.buf, authority, pathEnd-authority);

	// maybe add '/'
	if (addSlash)
	    md.update((byte)'/');

	// query
	if (query != -1)
	    md.update(bss.buf, query, bss.end-query);

	digest = md.digest();
	for (int i=0; i<8; i++,lval<<=8)
	    lval |= (0xFFL & digest[i]);
	return lval;
    }

    private static String urls [] = {
	"none",
	"http:",
	"http:/",
	"http:/www.foo.com/",
	"http:/www.foo.com/foo/",
	"http:/www.foo.com/foo/wow",
	"http://www.foo.com/",
	"http://www.foo.com",
	"http://foo.com",
	"http://foo.com/",
	"http://www.franklin.ma.us/auto/schools/FPS/pps/sepac/events",
	"http://www.franklin.ma.us/auto/schools/FPS/pps/sepac/foo/../events",
	"http://www.franklin.ma.us/auto/schools/FPS/pps/sepac/events/",
	"http://www.franklin.ma.us/auto/schools/FPS/pps/sepac/events/default.htm",
	"http://www.franklin.ma.us/auto/schools/FPS/pps/sepac/events/default.htm#foo",
	"http://www.franklin.ma.us/auto/schools/FPS/pps/sepac/events/default.htm?query=wow",
	"http://www.franklin.ma.us/auto/schools/FPS/pps/sepac/events/default.htm?query=wow#foo",
	"http://ncaa.thetask.com/market/jobs/volleyball/index.php",
	"http://ncaa.thetask.com/market/jobs/volleyball/index.php#foobar",
	"http://ncaa.thetask.com/market/jobs/volleyball/index.php?query=wow#foobar",
	"http://ncaa.thetask.com/market/jobs/volleyball/index.php?query=wow",
	"http://ncaa.thetask.com/market/jobs/volleyball/",
	"http://ncaa.thetask.com/market/jobs/volleyball",
	"http://ncaa.thetask.com/market/jobs/volleyball/?query=foo",
	"http://ncaa.thetask.com/market/jobs/volleyball/?query=foo#wow",
	"http://ncaa.thetask.com/market/jobs/volleyball?query=foo",
	"http://ncaa.thetask.com/market/jobs/volleyball?query=foo#wow"
    };

    public static void main(String[] args) throws URISyntaxException {
	MessageDigest md;
	ByteScan.State bss = new ByteScan.State();
	URI uri;
	String uriStr = null;

	try {
	    md = MessageDigest.getInstance("MD5");
	}
	catch (NoSuchAlgorithmException e) {
	    e.printStackTrace();
	    return;
	}

	for (int i=0; i<urls.length; i++) {
	    try {
		uri = new URI(urls[i]);
		uri = uri.normalize();
		uriStr = uri.toString();
		bss.init(uriStr.getBytes());
	    }
	    catch (URISyntaxException e) {
		System.out.println("URI Syntax Exception: " + uriStr);
		bss.init(urls[i].getBytes());
	    }
	    System.out.println(ComputeHash(bss, md) + "\t" + urls[i]);
	}
	
    }
    

}


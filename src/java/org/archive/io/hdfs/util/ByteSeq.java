/* LinkExtractor
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
package org.archive.io.hdfs.util;

/**
 * 
 */
public class ByteSeq {
    public ByteSeq() {
	init(null, 0, 0);
    }
    public ByteSeq(byte [] buf, int offset, int end) {
	init(buf, offset, end);
    }
    public void init(byte [] buf, int offset, int end) {
	this.buf = buf;
	this.offset = offset;
	this.end = end;
	caseSensitive = false;
    }
    public void init(byte [] buf) {
	this.buf = buf;
	this.offset = 0;
	this.end = buf.length;
	caseSensitive = false;
    }
    public String toString() { return new String(buf, offset, end-offset); }
    public byte [] buf;
    public int offset;
    public int end;
    public boolean caseSensitive;

}

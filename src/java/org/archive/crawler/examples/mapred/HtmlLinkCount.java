/* HtmlLinkCount
 *
 * $Id$
 *
 * Created on January 31st, 2007
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

package org.archive.crawler.examples.mapred;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import org.archive.crawler.writer.HDFSWriterDocument;

import org.archive.crawler.writer.util.ByteScan;
import org.archive.crawler.writer.util.ByteSeq;
import org.archive.crawler.writer.util.HTML;


/**
 * This Map/Reduce application that counts each unique URL
 * encountered in the HTML documents of a Heritrix crawl.
 *
 * @author Doug Judd
 */
public class HtmlLinkCount {

  /**
   * If character encoding can be determined, emits it as
   * (<b>charset</b>, <b>1</b>).
   */
  public static class MapClass extends MapReduceBase implements Mapper {

      private static char [] anchorElementData  = { 'a' };
      private static char [] headElementData    = { 'h','e','a','d' };
      private static char [] baseElementData    = { 'b','a','s','e' };
      private static char [] hrefData = { 'h','r','e','f' };
      private static char [] httpSchemeData = { 'h','t','t','p',':' };

      private final static LongWritable one = new LongWritable(1);
      private Text linkText = new Text();
      private ByteScan.State bss = new ByteScan.State();
      private HTML.ElementParser eparse = new HTML.ElementParser();

      private HDFSWriterDocument hdfsDoc = new HDFSWriterDocument();
      private ByteSeq attrKey = new ByteSeq();
      private ByteSeq attrValue = new ByteSeq();
      private ByteScan.State content = new ByteScan.State();
      private URI baseURI = null;
      private URI resolvedURI;
      private String uriStr = null;

      private boolean dropInternal = false;
      private boolean httpOnly = false;

      /**
       * Get page title to look for and initialize parser
       */
      public void configure(JobConf job) {
        String str = job.get("org.archive.crawler.exampes.mapred.HtmlLinkCount.dropInternal");
	if (str != null && str.toLowerCase().equals("true"))
	  dropInternal = true;
	str = job.get("org.archive.crawler.exampes.mapred.HtmlLinkCount.httpOnly");
	if (str != null && str.toLowerCase().equals("true"))
	  httpOnly = true;
      }

      public void map(WritableComparable key, Writable value,  
		      OutputCollector output, 
		      Reporter reporter) throws IOException {
	  Text docText = (Text)value;
	  Text urlText = (Text)key;
	  String thisUrlStr;
	  String charset;
	  String outputUrlStr;
	  int    fragIndex;
	  String thisHost;

	  /**
	  ByteArrayInputStream bais = new ByteArrayInputStream(docText.getBytes(), 0, docText.getLength());
	  DataInputStream dis = new DataInputStream(bais);
	  hdfsDoc.readFields(dis);
	  */

	  // load document object (skips initial 4-byte length)
	  hdfsDoc.load(docText.getBytes(), 4, docText.getLength()-4);

	  // only consider 200 responses
	  if (hdfsDoc.getResponseCode() != 200)
	    return;

	  // Only consider HTML
	  if (!"text/html".equals(hdfsDoc.getContentType()))
	    return;

	  charset = hdfsDoc.getValidCharset();
	  thisUrlStr = new String(urlText.getBytes(), 0, urlText.getLength(), charset);

	  if (hdfsDoc.getResponseLength() == 0)
	    return;

	  bss.init(hdfsDoc.getResponseBytes(), hdfsDoc.getResponseOffset(),
		   hdfsDoc.getResponseOffset() + hdfsDoc.getResponseLength());

	  uriStr = null;
	  baseURI = null;

	  try {

	    eparse.init(bss, headElementData);
	    if (eparse.Find()) {
	      eparse.SkipAttributes();
	      if (eparse.GetContent(content)) {
		eparse.init(content, baseElementData);
		if (eparse.Find()) {
		  while (eparse.NextAttribute(attrKey, attrValue)) {
		    if (ByteScan.Equals(attrKey, hrefData)) {
		      uriStr = new String(attrValue.buf, attrValue.offset, attrValue.end-attrValue.offset, charset);
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
	      baseURI = new URI(thisUrlStr);
	  }
	  catch (URISyntaxException e) {
	    return;
	  }

	  if (httpOnly && !"http".equals(baseURI.getScheme()))
	    return;

	  if ((thisHost = baseURI.getHost()) == null)
	    return;

	  // strip fragment
	  outputUrlStr = baseURI.toString();
	  if ((fragIndex = outputUrlStr.indexOf('#')) > 0)
	    outputUrlStr = outputUrlStr.substring(0, fragIndex);

	  // output key URL
	  linkText.set(outputUrlStr);
	  output.collect(linkText, one);

	  bss.init(hdfsDoc.getResponseBytes(), hdfsDoc.getResponseOffset(),
		   hdfsDoc.getResponseOffset() + hdfsDoc.getResponseLength());
	  eparse.init(bss, anchorElementData);

	  while (eparse.Find()) {
	    //System.out.println();
	    while (eparse.NextAttribute(attrKey, attrValue)) {
	      if (ByteScan.Equals(attrKey, hrefData)) {
		uriStr = new String(attrValue.buf, attrValue.offset, attrValue.end-attrValue.offset, charset);
		try {
		  if (hasScheme(attrValue)) {
		    resolvedURI = new URI(uriStr);
		    String path = resolvedURI.getPath();
		    if (path == null || path.length() == 0)
		      resolvedURI = resolvedURI.resolve("/");
		    else
		      resolvedURI = resolvedURI.normalize();
		    if (httpOnly && !"http".equals(resolvedURI.getScheme()))
		      continue;
		    if (dropInternal && thisHost.equals(resolvedURI.getHost()))
		      continue;
		  }
		  else if (!dropInternal)
		    resolvedURI = baseURI.resolve(uriStr);
		  else
		    continue;

		  outputUrlStr = resolvedURI.toString();
		  
		  // strip fragment
		  if ((fragIndex = outputUrlStr.indexOf('#')) > 0)
		    outputUrlStr = outputUrlStr.substring(0, fragIndex);

		  linkText.set(outputUrlStr);
		  output.collect(linkText, one);
		}
		catch (Exception e) {
		  //System.err.println("URI Syntax Exception:  " + uriStr);
		}
	      }
	    }
	  }
      }

      /**
       * Check to see if link has a scheme component
       */
      private boolean hasScheme(ByteSeq url) {
	for (int offset = url.offset; offset < url.end; offset++) {
	  if (url.buf[offset] == '.' || url.buf[offset] == '/')
	    return false;
	  else if (url.buf[offset] == ':')
	    return true;
	}
	return false;
      }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class Reduce extends MapReduceBase implements Reducer {

    private boolean excludeSingles = false;

    /**
     * Get page title to look for and initialize parser
     */
    public void configure(JobConf job) {
      String str = job.get("org.archive.crawler.exampes.mapred.HtmlLinkCount.excludeSingles");
      if (str != null && str.toLowerCase().equals("true"))
	excludeSingles = true;
    }

    public void reduce(WritableComparable key, Iterator values,    
		       OutputCollector output,
		       Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += ((LongWritable) values.next()).get();
      }
      if (!excludeSingles || sum > 1)
	output.collect((WritableComparable) key, new LongWritable(sum));
    }
  }

  static void printUsage() {
    System.out.println("HtmlLinkCount [OPTIONS] <output> <input> [<input> ...]");
    System.out.println();
    System.out.println("OPTIONS:");
    System.out.println("-m <maps>          set nmber of map tasks to <maps>");
    System.out.println("-r <reduces>       set number of reduce tasks to <reduces>");
    System.out.println("--exclude-singles  don't emit links of count 1 in reduce()");
    System.out.println("--external-only    only count external links");
    System.out.println("--http-only        only count links with scheme http");
    System.out.println();
    System.exit(1);
  }
  
  /**
   * The main driver for HtmlLinkCount map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public static void main(String[] args) throws IOException {
    JobConf conf = new JobConf(HtmlLinkCount.class);
    conf.setJobName("htmllinkcount");
	  
    conf.setInputFormat(SequenceFileInputFormat.class);

    // the output keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the output values are counts (ints)
    conf.setOutputValueClass(LongWritable.class);
    
    conf.setMapperClass(MapClass.class);        
    conf.setCombinerClass(LongSumReducer.class);
    conf.setReducerClass(Reduce.class);

    List other_args = new ArrayList();
    for(int i=0; i < args.length; ++i) {
      try {
	if ("-m".equals(args[i])) {
	  conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
	} else if ("--exclude-singles".equals(args[i])) {
	  conf.set("org.archive.crawler.exampes.mapred.HtmlLinkCount.excludeSingles", "true");
	} else if ("--external-only".equals(args[i])) {
	  conf.set("org.archive.crawler.exampes.mapred.HtmlLinkCount.dropInternal", "true");
	} else if ("--http-only".equals(args[i])) {
	  conf.set("org.archive.crawler.exampes.mapred.HtmlLinkCount.httpOnly", "true");
	}else {
	  other_args.add(args[i]);
	}
      } catch (NumberFormatException except) {
	System.out.println("ERROR: Integer expected instead of " + args[i]);
	printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
	System.out.println("ERROR: Required parameter missing from " +
			   args[i-1]);
	printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() < 2) {
      System.out.println("ERROR: Wrong number of parameters: ");
      printUsage();
    }
    conf.setOutputPath(new Path((String) other_args.get(0)));
    for (int i=1; i<other_args.size(); i++)
      conf.addInputPath(new Path((String) other_args.get(i)));
    
    // Uncomment to run locally in a single process
    // conf.set("mapred.job.tracker", "local");
    
    JobClient.runJob(conf);
  }
}

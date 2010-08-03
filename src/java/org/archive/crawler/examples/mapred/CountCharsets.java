/* CountCharsets
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
import java.util.ArrayList;
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


/**
 * This Map/Reduce application generates counts for each unique
 * character encoding (charset) encountered in a Heritrix crawl.
 *
 * @author Doug Judd
 */
public class CountCharsets {

  /**
   * If character encoding can be determined, emits it as
   * (<b>charset</b>, <b>1</b>).
   */
  public static class MapClass extends MapReduceBase implements Mapper {

      private final static LongWritable one = new LongWritable(1);
      private Text cset = new Text();

      private HDFSWriterDocument hdfsDoc = new HDFSWriterDocument();

      public void map(WritableComparable key, Writable value, 
		      OutputCollector output, 
		      Reporter reporter) throws IOException {
	  Text docText = (Text)value;

	  /**
	  ByteArrayInputStream bais = new ByteArrayInputStream(docText.getBytes(), 0, docText.getLength());
	  DataInputStream dis = new DataInputStream(bais);
	  hdfsDoc.readFields(dis);
	  */

	  // load document object (skips initial 4-byte length)
	  hdfsDoc.load(docText.getBytes(), 4, docText.getLength()-4);

	  if (hdfsDoc.getCharset() != null) {
	      cset.set(hdfsDoc.getCharset());
	      output.collect(cset, one);
	  }
      }
  }

  static void printUsage() {
    System.out.println("CountCharsets [-m <maps>] <output> <input> [<input> ...]");
    System.exit(1);
  }
  
  /**
   * The main driver for CountCharsets map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public static void main(String[] args) throws IOException {
    JobConf conf = new JobConf(CountCharsets.class);
    conf.setJobName("countcharsets");
	  
    conf.setInputFormat(SequenceFileInputFormat.class);

    // the output keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the output values are counts (ints)
    conf.setOutputValueClass(LongWritable.class);
    
    conf.setMapperClass(MapClass.class);        
    conf.setCombinerClass(LongSumReducer.class);
    conf.setReducerClass(LongSumReducer.class);

    conf.setNumReduceTasks(1);
    
    List other_args = new ArrayList();
    for(int i=0; i < args.length; ++i) {
      try {
	if ("-m".equals(args[i])) {
	  conf.setNumMapTasks(Integer.parseInt(args[++i]));
	} else {
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

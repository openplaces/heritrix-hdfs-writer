
heritrix-hadoop-dfs-writer-processor-2.0.1

Created on January 20th, 2007

Copyright (C) 2008 Zvents

This file is part of the Heritrix web crawler (crawler.archive.org).

Heritrix is free software; you can redistribute it and/or modify
it under the terms of the GNU Lesser Public License as published by
the Free Software Foundation; either version 2.1 of the License, or
any later version.

Heritrix is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser Public License for more details.

You should have received a copy of the GNU Lesser Public License
along with Heritrix; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA


Doug Judd
Zvents, Inc.
doug zvents.com


TABLE OF CONTENTS
=================
* SETUP
* CONFIGURING HERITRIX
* FILE FORMAT
* COMPILING THE SOURCE
* RUNNING AN EXAMPLE MAP-REDUCE PROGRAM
* HELPFUL HINTS

The heritrix-hadoop-dfs-writer-processor is an extension to the Heritrix open
source crawler written by the Internet Archive (http://crawler.archive.org/)
that enables it to store crawled content directly into HDFS, the Hadoop
Distributed FileSystem (http://lucene.apache.org/hadoop/).  Hadoop implements
the Map/Reduce distributed computation framework on top of HDFS.  
heritrix-hadoop-dfs-writer-processor writes crawled content into SequenceFile
format which is directly supported by the Map/Reduce framework and has support
for compression.  This facilitates running high-speed, distributed computations
over content crawled with Heritrix.

The current version of heritrix-hadoop-dfs-writer-processor assumes version
2.0 of Heritrix and version 0.16.4 of Hadoop.  Newer versions of Hadoop
and Heritrix may continue to work with this connector as long as the pertinent
APIs have not changed.  Just replace the jar files with the newer versions.


SETUP
=====

1. Start an instance of hadoop-0.16.4
2. Install heritrix-2.0
3. Untar the current distribution to any directory and once its untarred enter the
   <HDFSWriterProcessor_home> directory and enter ant jar.
4. Copy the following jar files from the heritrix-hadoop-dfs-writer-processor binary
   distribution into the lib/ directory of your Heritrix installation:
	 a) heritrix-hadoop-dfs-writer-processor-2.0.1.jar
  	 b) hadoop-0.16.4-core.jar
     c) log4j-1.2.13.jar (from the hadoop distribution)
5. Copy all the subdirectories from within <HDFSWriterProcessor_home>/sample_profiles
   to <Heritrix-2.0>/jobs.
6. Start Heritrix


CONFIGURING HERITRIX
====================

- On the Web-UI click on Engine ID which will open up the List of Profiles.
- From the Profiles, select Hbase_outlinks or Hbase_no_outlinks and copy to a ready to run job.
- Once the job is copied navigate to "Edit-Sheet" by clicking on sheets-->edit
- In the sheet edit page, select HDFSWriterProcessor as the selected Archiver.
  (NOTE: from the drop-down menu..)
- Now in the same page fill the following fields,


	-hdfs-fs-default-name
	  This should be set to the same string as the fs.default.name parameter in the
	   hadoop-site.xml file
	
	-hdfs-output-path
	  This is the base directory into which the crawled documents will get written.
	  A subdirectory that has the same name as the one that is created in the
	  heritrix/jobs directory will get created here.  Inside this subdirectory, the
	  SequenceFiles will get written.
	
	-hdfs-compression-type
	  This is the type of compression to use in the Sequence files.  The possible
	  values are DEFAULT, BLOCK, RECORD, and NONE.
	
	-hdfs-replication
	  This is the replication factor for files written into HDFS
	  (NOTE: this currently has no effect, see JIRA bug HADOOP-907)

- Click on save changes and commit and launchthe job.


FILE FORMAT
===========

The value portion of the SequenceFiles that are generated have the following
format:

  HDFSWriter/0.1
  <name-value-parameters>
  CRLF
  <http-request> (only for http scheme)
  CRLF
  <http-response-headers> (only for http scheme)
  CRLF
  <response-body>

The following example, illustrates the format:

HDFSWriter/0.1
URL: http://www.cnn.com/.element/ssi/www/sect/1.3/misc/contextual/MAIN.html
Ip-Address: 64.236.29.120
Crawl-Time: 20070123093916
Is-Seed: false
Path-From-Seed: X
Via: http://www.cnn.com/

GET /.element/ssi/www/sect/1.3/misc/contextual/MAIN.html HTTP/1.0
User-Agent: Mozilla/5.0 (compatible; heritrix/1.12.0 +http://www.zvents.com/)
From: crawler@zvents.com
Connection: close
Referer: http://www.cnn.com/
Host: www.cnn.com
Cookie: CNNid=46e19fc2-12419-1169545061-167

HTTP/1.1 200 OK
Date: Tue, 23 Jan 2007 09:37:46 GMT
Server: Apache
Vary: Accept-Encoding,User-Agent
Cache-Control: max-age=60, private
Expires: Tue, 23 Jan 2007 09:38:46 GMT
Content-Length: 3489
Content-Type: text/html
Connection: close

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
        "http://www.w3.org/TR/html4/loose.dtd">
<html lang="en">
<head>
	<meta http-equiv="content-type" content="text/html; charset=iso-8859-1">
	<title>Link Spots CSI</title>
	<script type="text/javascript">
[...]


COMPILING THE SOURCE
====================

Run the following commands:

tar xzvf heritrix-hadoop-dfs-writer-processor-2.0.1.tar.gz
cd heritrix-hadoop-dfs-writer-processor-2.0.1
ant jar

The jar file will end up in the build/ subdirectory.  To see the new HDFS
writer processor in eclipse, you need to add the
heritrix-hadoop-dfs-writer-processor-2.0.1.jar in the 'Java build path -> Libraries'
 panel.


RUNNING AN EXAMPLE MAP-REDUCE PROGRAM
=====================================

The binary and source distributions come with an example map-reduce program
called org.archive.crawler.examples.mapred.CoutCharsets that produces counts
for all of the unique character encodings (charsets) encountered in your
crawled documents.  The source code for this example can be found in the file
src/java/org/archive/crawler/examples/mapred/CountCharsets.java in the
source distribution.

To run this example program, do the following.

1. Copy the heritrix-hadoop-dfs-writer-processor-2.0.1.jar file into the lib/
   directory of your hadoop-0.12.2 installation.  (NOTE: You should
   push this jar file into the lib directory of all participating
   Hadoop nodes)
2. cd into the hadoop-0.12.2 directory
3. Invoke the map-reduce jobs with a line like the following:

$ ./bin/hadoop org.archive.crawler.examples.mapred.CountCharsets /output \
      /heritrix/crawls/no-extract-5-20070130081658484

(Be sure to change the final agument in the above line to your Heritrix
 output directory)

This should generate a file in HDFS called /output/part-00000 that contains a
number of lines, one for each unique character set encountered, containing the
name of the character set followed a count.  To see the result, run the
following commands.

$ ./bin/hadoop dfs -copyToLocal /output/part-00000
$ cat part-00000


HELPFUL HINTS
=============

1. To prevent the crawler from crawling media files, do the following

   On the Submodules page, under pre-fetch-processors -> preselector -> decide-rules,
   add the following two decide rules:

     - acceptByDefault - The AcceptDecideRule that accepts everything by default
     - dropMedia - MatchesFilePatternDecideRule - this will drop media files

   On the Settings page under pre-fetch-processors -> Preselector -> filters ...
   dropMedia setting, set the decision to REJECT and the use-preset-pattern to All

   NOTE: by putting this filter in the pre-fetch-processor, it will get applied to
   the seeds as well.  If you don't want it applied to the seeds, add the filter
   to the crawl-scope module.

2. To prevent the crawler from writing dns, robots.txt, .css, and .js files to
   the content store, do the following

   On the Submodules page, under write-processors -> HDFSArchiver -> decide-rules,
   add an AcceptDecideRule (called acceptByDefault) and a MatchesFilePatternDecideRule
   (called dropMeta).

   On the Settings page under the write-processors -> HDFSArchiver section,
   modify the decide-rules -> rules -> dropMeta rule setting as follows:

     - set the decision to REJECT
     - set the use-preset-pattern to Custom
     - set the regexp to ^((dns:.*)|(.*(robots.txt|css|js)$))

# GraphChi-DB

GraphChi-DB is a scalable, embedded, single-computer online graph database that can also execute similar large-scale graph computation as [GraphChi](https://github.com/graphchi).
it has been developed by [Aapo Kyrola](http://www.cs.cmu.edu/~akyrola) as part of his Ph.D. thesis.

It can handle graphs with billions of edges on just a laptop or PC, fast!

GraphChi-DB is written in Scala, with some Java code. Generally, you need to know Scala quite well to be able to use it.

**IMPORTANT: GraphChi-DB is early release, research code. It is buggy, it has awful API, and it is provided with no guarantees.
DO NOT USE IT FOR ANYTHING IMPORTANT.**


## License

GraphChi-DB is licensed under the Apache License, Version 2.0.
See any source code file for full license information.

## Requirements

* Solid-state Drive (SSD)  (for good query performance; hard drive might work well if enough RAM)
* 8 GB of RAM is recommended. Might work with less.
* Scala 2.9

 ## Discussion group

 Please join the discussion group (shared with GraphChi users).

 http://groups.google.com/group/graphchi-discuss


 ## Publication

You can read about GraphChi-DB's design, evaluation and motivation from pre-print [GraphChi-DB: Simple Design For a Scalable Graph Database System - on Just a PC](http://arxiv.org/abs/1403.0701).

# Documentation

Not provided. We suggest you look through the examples (see below), to get the basic idea.

# Examples

**In general, you must be very familiar with Scala and Java development to be able to use GraphChi-DB.** 

## To run

Best way to explore GraphChi-DB is to load the project into an IDE (such as Eclipse or IntelliJ IDEA), and use the Scala Console. This will allow you to interactively explore the data.
You can also include GraphChi-DB easily in your Scala project.

Following JVM parameters are recommended:
```
   -Xmx5G -ea
```

**Note:** With less than 5 gigabyte of RAM, the database may crash (silently) in an out-of-memory exception. This is because its buffers overflow, and the database cannot yet manage its own memory usage.
However, do NOT add more memory to the JVM, because GraphChi-DB uses memory mapping of the operating system to manage the data. It is better to leave as much memory for the OS to use for memory mapping.


## Example: Social Network

Source: https://github.com/GraphChi/graphchiDB-scala/blob/master/src/main/scala/edu/cmu/graphchidb/examples/SocialNetworkExample.scala

This example creates a graph from a social network data. For each edge, we add a timestamp and weight column. In this example, this values
are populated with random values, so they are just provided as an example.

In addition, the example computes continously Pagerank for each vertex. You can also invoke a connected components computation. Note that these computations operate in the background and if the graph changes, also their results can become incorrect. 

This example shows also how to compute simple social network recommendations based on friends-of-friends. 

### Input data

To run this example, you need to have some input graph. You can try these:
* Live Journal: http://snap.stanford.edu/data/soc-LiveJournal1.html
* Twitter graph (2010): http://an.kaist.ac.kr/traces/WWW2010.html

Remember to configure the proper filenames in the code. Look for variable "sourceFile".
Also set the "numShards" parameter properly. Based on the expected number of edges, use one shard / 5 million edges. That is, for 1 billion edgers, use 200 shards. 


### Example session

```scala
   import  edu.cmu.graphchidb.examples.SocialNetworkExample._

   // To initialize DB (you need to do this only on your first session)
   startIngest

   // Some testing
   recommendFriends(8737)
   recommendFriends(2419)
   recommendFriendsLimited(2419)
   
   // In and out neighbors
   DB.queryIn(DB.originalToInternalId(2409), 0)
   DB.queryOut(DB.originalToInternalId(8737), 0)

   // To run connected components
   connectedComponents()

   // After a while, you can ask
   ccAlgo.printStats

   // To get a vertex component label (which might not be yet the final one)
   ccAlgo.vertexDataColumn.get.get(DB.originalToInternalId(8737)).get

   // To get pagerank of a vertex (note, that it is being continuously updated), so this
   // just looks up the value.
   pagerankCol.get(DB.originalToInternalId(8737)).get
```

### Advanced Social Network Analysis

Source: https://github.com/GraphChi/graphchiDB-scala/blob/master/src/main/scala/edu/cmu/graphchidb/examples/SubgraphFrequencies.scala

Application __SubgraphFrequencies__ allows reproducing some of the results in paper [Ugander, Backstrom, Kleinberg: "Subgraph frequencies: Mapping the empirical and extremal geography of large graph collections"] (http://www2013.org/proceedings/p1307.pdf).

Given a graph (social network), it will sample a set of vertices, and for each of the vertices load the induced subgraph of the vertex's neighbors (excluding itself).
From each induced neighborhood graph, we sample thousands of times three vertices and record the type of subgraph induced by the three vertices.
The frequencies are output to a file that can be plotted using R or Matlab to see the subgraph frequency distribution of the graph.

Note: you need to first create the graph (see previous example).

Example session:
```scala
   // Usage:
       import edu.cmu.graphchidb.examples.SubgraphFrequencies._

       // To compute subgraph freqs of a given vertex neighborhood
       computeThreeVertexSubgraphFrequencies(inducedNeighborhoodGraph(2419))

      // To produce data similar to used in Figure 1 of Ugander et. al.:
       computeDistribution(500)
   )

```

The source code contains a sample R script to plot the distribution.




## Example: Wikipedia Graph

Source: https://github.com/GraphChi/graphchiDB-scala/blob/master/src/main/scala/edu/cmu/graphchidb/examples/WikipediaGraph.scala

This example application reads Wikipedia's SQL dumps and creates a graph of the wikipedia pages. The process takes a while (a couple of hours)
because the program needs to resolve page names to page IDs.

It allows you to then find shortest paths between pages.
You can consider extending it with Pagerank, Connected Components etc. by using techniques from the previous example.

### Input data

Wikipedia dumps are available here: http://dumps.wikimedia.org/enwiki/latest/

You need **two** files: 
* Pagelinks: for example http://dumps.wikimedia.org/enwiki/20140402/enwiki-20140402-pagelinks.sql.gz
* Per-page data: http://dumps.wikimedia.org/enwiki/20140402/enwiki-20140402-page.sql.gz

The example application includes a very hacky, but very fast, parser for the data. Note that it has been tested only with the English wikipedia data.


### Example session: Ingest

You need to run this only on your first session:
```scala
  import  edu.cmu.graphchidb.examples.WikipediaGraph._

  // If first time, populate the DB (takes 4 hours on SSD, MacBook Pro)
  populate()
```

### Example session: Play

Note, that the first query will take a long time as the application needs to compute the page title index. Also, it takes time before the graph is fully cached (using memory mapping).

```scala
  import  edu.cmu.graphchidb.examples.WikipediaGraph._
  shortestPath("Barack_Obama", "Sauli_Niinisto")
  shortestPath("Helsinki", "Pittsburgh")
  shortestPath("Carnegie_Mellon_University", "Graph")
  shortestPath("Rabbit", "Empire_State_Building")
```

## Example: Movie Database and Recommender

Source: https://github.com/GraphChi/graphchiDB-scala/blob/master/src/main/scala/edu/cmu/graphchidb/examples/MovieDatabase.scala

This example creates a database of movies and a graph of user - movie ratings. After the database has been populated, you can run matrix factorization using the Alternating Least Squares (ALS) algorithm and use it to recommend movies to a user. Note, that this is overly simple recommender algorithm and unlikely to produce very good results.

### Data

You can get Netflix Prize movie rating database from here:  http://www.select.cs.cmu.edu/code/graphlab/datasets/

Download the "netflix_mm" file.


### Example session

Ingest:
```scala
 import edu.cmu.graphchidb.examples.MovieDatabase._
  startIngest
```

Recommend:
```scala
  import edu.cmu.graphchidb.examples.MovieDatabase._
  recommendForUser(X)
```

### Notes
This example is a bit awkward: GraphChi-DB does not support typed vertices, so we need to use different range of IDs for movie and user vertices. Variable "userIdOffset" is used for this. The example functions in the app include the translation.


# Notes

## Online

An added edge is immediately visible to queries and subsequent computation. That is, you can query the database while inserting new data.
There is currently no batch mode to insert edges. Still, even in the online mode you can expect to be able to insert over 100K edges per second.

## ID-mapping

GraphChi-DB maps original vertex IDs to an internal ID space. Functions that ask for "origId" expect you to provide the original vertex ID (from the source data).
Typically you need to map IDs returned by the database into original IDs. See the example applications for examples. The GraphChiDB class has two functions to map between the ID spaces:

```scala
DB.originalToInternalId(origId)
DB.internalToOriginalId(internalId)
```

This mapping is awkward, but crucial for the performance of the database. See the publication for more information.


## Crash?

Sometimes GraphChi-DB seems to get silently stuck. This is caused by an Out-of-Memory error that is not being, for some reason, thrown and logged.
You should allocate 5 gigabytes of RAM to the JVM: -Xmx5G

## Durability

New edges are first added to in-memory buffers. When the buffers are full, they are flushed to disk. If the computer crashes when the edges are in buffers, they are lost!
GraphChi-DB has also a durable mode which logs all edges to a file before adding them to buffers (see the configuration file). However, there is currently no method to recover after a crash.

GraphChi-DB uses a shutdown hook to flush the buffered edges to disk. It is important not to force-kill the JVM! You can also manually call

```java
DB.flushAllBuffers()

```

Again, see the example applications...


## Number of shards

When creating a database, you need to specify the number of shards. A rule of thumb: if you expect N edges, use N / 5 million shards. So for 1 billion edges, use 200 shards, for 5 billion, maybe 500 is fine. 

## Debug log

Inside the database directory, there is a debug log-file that is written by the database.

```
   tail -f *debug.txt*
```

## Deleting database

To delete your database, just remove the directory containing the database. You need to restart your application as well.

## Native Library for Sorting

To get better performance, you can enable a native library for sort operations. It is provided in binary under lib/, and can be compiled from src/jni/ (although I have had trouble compiling it on Linux).

To enable, add this to the command line:
```
  ... -Djava.library.path=lib/
```


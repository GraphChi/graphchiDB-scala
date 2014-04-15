# GraphChi-DB

GraphChi-DB is a scalable, embedded, single-computer graph database that can also execute similar large-scale graph computation as [GraphChi](https://github.com/graphchi).
it has been developed by [Aapo Kyrola](http://www.cs.cmu.edu/~akyrola) as part of his Ph.D. thesis.

GraphChi-DB is written in Scala, with some Java code. Generally, you need to know Scala quite well to be able to use it.

** IMPORTANT: GraphChi-DB is early release, research code. It is buggy, it has awful API, and it is provided with no guarantees.
DO NOT USE IT FOR ANYTHING IMPORTANT.  **


## License

GraphChi-DB is licensed under the Apache License, Version 2.0
Each source code file has full license information.

## Requirements

* SSD
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

## To run

Best way to explore GraphChi-DB is to load the project into an IDE (such as Eclipse or IntelliJ IDEA), and use the Scala Console. This will allow you to interactively explore the data.
You can also include GraphChi-DB easily in your Scala project.

Following JVM parameters are recommended:
```
   -Xmx5G -ea
```

**Note: ** With less than 5 gigabyte of RAM, the database may crash (silently) in an out-of-memory exception. This is because its buffers overflow, and the database cannot yet manage its own memory usage.
However, do NOT add more memory to the JVM, because GraphChi-DB uses memory mapping of the operating system to manage the data. It is better to leave as much memory for the OS to use for memory mapping.




# Notes

## ID-mapping

## Durability

##

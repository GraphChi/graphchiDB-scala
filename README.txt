 mvn install:install-file -Dfile=lib/FacebookLinkBench.jar -DgroupId=com.facebook.linkbench -DartifactId=FacebookLinkBench -Dversion=1.0  -Dpackaging=jar

 mvn install:install-file -Dfile=lib/GDBenchmark.jar -DgroupId=gdbench -DartifactId=GdBench -Dversion=1.0  -Dpackaging=jar

## Building 

You can build GraphChi with any IDE, or using Maven. Just
write on the command-line:
```
     mvn assembly:assembly -DdescriptorId=jar-with-dependencies
 
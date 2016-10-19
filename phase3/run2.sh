javac -cp '/usr/lib/hadoop-0.20/hadoop-core-0.20.2-cdh3u2.jar' -d moralize_classes  Moralization.java JunctionTreeConstruction.java Moralize.java ExampleBaseJob.java MST.java
jar -cvf moralize.jar -C moralize_classes/ .
hadoop jar moralize.jar org.myorg.Moralize

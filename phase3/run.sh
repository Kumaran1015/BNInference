hadoop fs -rmr moralize/output
hadoop fs -rmr moralize/inter
hadoop fs -rmr moralize/sorting_output
hadoop fs -rmr joinadj/output
hadoop fs -rmr maximal_clique/output
hadoop fs -rmr junction_tree/output
hadoop fs -rmr junction_tree/output2
hadoop fs -rmr junction_tree/sorted_output
hadoop fs -rmr junction_tree/final_output
javac -cp '/usr/lib/hadoop-0.20/hadoop-core-0.20.2-cdh3u2.jar' -d moralize_classes  Moralize.java ExampleBaseJob.java MST.java
jar -cvf moralize.jar -C moralize_classes/ .
hadoop jar moralize.jar org.myorg.Moralize /user/training/moralize/input /user/training/moralize/inter /user/training/moralize/output

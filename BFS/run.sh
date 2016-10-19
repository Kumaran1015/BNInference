javac -cp '/usr/lib/hadoop-0.20/hadoop-core-0.20.2-cdh3u2.jar' -d bfs_classes  ExampleBaseJob.java Node.java SearchMapper.java	SearchReducer.java SSSPJob.java
jar -cvf bfs.jar -C bfs_classes/ .
hadoop jar bfs.jar HadoopGTK.SSSPJob /user/training/bfs/input /user/training/bfs/output

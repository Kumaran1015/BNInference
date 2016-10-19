export hadoopClassPath=`hadoop classpath`
#echo $hadoopClassPath
javac -cp $hadoopClassPath -d runner_classes  Moralization.java JunctionTreeConstruction.java Runner.java ExampleBaseJob.java MST.java
jar -cvf runner.jar -C runner_classes/ .
hadoop jar runner.jar org.myorg.Runner ../../testInput1/file01

Instructions to run the ChessPieceValidation MapReduce program:

1. Compile the Java file:
    javac -classpath `hadoop classpath` -source 11 -target 11 ChessPieceValidator.java  
   --here release 11 mean compatible version of java 

2. Package the compiled classes into a jar file:
   jar -cvf ChessPieceValidation.jar ChessPieceValidator*.class 

3. Run the WordCount program:
   hadoop jar wordcount.jar WordCount <input_path> <output_path>

   Example:
   % hadoop jar ChessPieceValidation.jar ChessPieceValidator /Users/drpadhaya/Desktop/Chess_Validation/input.txt /Users/drpadhaya/Desktop/Chess_Validation/output
   ## Don't forget to use exact path  

4. Check the output:
   hadoop fs -cat <output_path>/part-r-00000
   Example
   cat /Users/drpadhaya/Desktop/Chess_Validation/output/part-r-00000 
5.Possible Error
  -Output file already present 
    -rm -r /path/to/output
  -Incorrect Java_Home
     -echo $JAVA_HOME
     -export JAVA_HOME=/path/to/java
  -Incorrect Hadoop_home
      -echo $HADOOP_HOME
      -export HADOOP_HOME=/opt/homebrew/opt/hadoop
      -export PATH=$PATH:$HADOOP_HOME/bin
      -export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop








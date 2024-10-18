<h1>Chess Piece Validation MapReduce Program</h1>

    <h2>Overview</h2>
    <p>
        The ChessPieceValidation program is a Java-based MapReduce application that validates a chessboard configuration.
        It processes an input file containing chess pieces, each represented by color, type, and position, and ensures that:
    </p>
    <ul>
        <li>All pieces are placed within valid positions on the chessboard (A1 to H8).</li>
        <li>The chess set contains the correct number of each piece (e.g., 1 King, 8 Pawns).</li>
        <li>No duplicate positions (i.e., multiple pieces in the same square) are present.</li>
        <li>Missing or extra pieces are reported.</li>
        <li>Any positions that deviate from the expected starting positions for the pieces are highlighted.</li>
    </ul>

    <h2>Prerequisites</h2>
    <p>
        Ensure that the following software is installed and properly configured on your system:
    </p>
    <ul>
        <li>Java 11</li>
        <li>Hadoop (installed and configured, with <code>$HADOOP_HOME</code> set)</li>
        <li>An input file containing chess pieces in the correct format.</li>
    </ul>

    <h2>Input File Format</h2>
    <p>Each line in the input file should follow this format:</p>
    <pre>&lt;Color&gt; &lt;Piece&gt; &lt;Position&gt;</pre>
    <p>Example:</p>
    <pre>White Rook A1<br>Black Pawn D7<br>White Knight B1</pre>

    <h2>Installation and Compilation</h2>

    <h3>Step 1: Compile the Java Program</h3>
    <p>Ensure that the <code>hadoop</code> classpath is available and you are using Java 11. Run the following command to compile the Java file:</p>
    <pre>javac -classpath `hadoop classpath` -source 11 -target 11 ChessPieceValidator.java</pre>

    <h3>Step 2: Package the Compiled Classes into a JAR File</h3>
    <p>Once the Java program is compiled, package the class files into a JAR file:</p>
    <pre>jar -cvf ChessPieceValidation.jar ChessPieceValidator*.class</pre>

    <h2>Running the Program</h2>

    <h3>Step 3: Execute the ChessPieceValidation Program</h3>
    <p>You can now run the program using Hadoop. Replace <code>&lt;input_path&gt;</code> and <code>&lt;output_path&gt;</code> with the actual paths to your input and output directories:</p>
    <pre>hadoop jar ChessPieceValidation.jar ChessPieceValidator &lt;input_path&gt; &lt;output_path&gt;</pre>
    <p>Example:</p>
    <pre>hadoop jar ChessPieceValidation.jar ChessPieceValidator /Users/drpadhaya/Desktop/Chess_Validation/input.txt /Users/drpadhaya/Desktop/Chess_Validation/output</pre>

    <h3>Step 4: Check the Output</h3>
    <p>After the program has run, you can check the output by either using the Hadoop filesystem command or by accessing the output file directly:</p>
    <pre>hadoop fs -cat &lt;output_path&gt;/part-r-00000</pre>
    <p>Example:</p>
    <pre>cat /Users/drpadhaya/Desktop/Chess_Validation/output/part-r-00000</pre>

    <h2>Troubleshooting</h2>

    <h3>Possible Errors</h3>
    <ul>
        <li><strong>Output file already exists:</strong>
            <p>If the output directory already exists, delete it before running the program again:</p>
            <pre>rm -r /path/to/output</pre>
        </li>
        <li><strong>Incorrect <code>JAVA_HOME</code>:</strong>
            <p>If you encounter issues with Java, verify your <code>JAVA_HOME</code> environment variable:</p>
            <pre>echo $JAVA_HOME<br>export JAVA_HOME=/path/to/java</pre>
        </li>
        <li><strong>Incorrect <code>HADOOP_HOME</code>:</strong>
            <p>If you encounter issues with Hadoop, ensure your <code>HADOOP_HOME</code> is set properly:</p>
            <pre>echo $HADOOP_HOME<br>export HADOOP_HOME=/opt/homebrew/opt/hadoop<br>export PATH=$PATH:$HADOOP_HOME/bin<br>export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop</pre>
        </li>
    </ul>

</div>

</body>
</html>

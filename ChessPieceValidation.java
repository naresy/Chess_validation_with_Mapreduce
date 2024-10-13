import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessPieceValidation {

    // Standard starting positions
    private static final Map<String, String> startingPositions = new HashMap<>();

    static {
        startingPositions.put("White King", "E1");
        startingPositions.put("White Queen", "D1");
        startingPositions.put("White Rook1", "A1");
        startingPositions.put("White Rook2", "H1");
        startingPositions.put("White Bishop1", "C1");
        startingPositions.put("White Bishop2", "F1");
        startingPositions.put("White Knight1", "B1");
        startingPositions.put("White Knight2", "G1");
        startingPositions.put("White Pawn1", "A2");
        startingPositions.put("White Pawn2", "B2");
        startingPositions.put("White Pawn3", "C2");
        startingPositions.put("White Pawn4", "D2");
        startingPositions.put("White Pawn5", "E2");
        startingPositions.put("White Pawn6", "F2");
        startingPositions.put("White Pawn7", "G2");
        startingPositions.put("White Pawn8", "H2");

        startingPositions.put("Black King", "E8");
        startingPositions.put("Black Queen", "D8");
        startingPositions.put("Black Rook1", "A8");
        startingPositions.put("Black Rook2", "H8");
        startingPositions.put("Black Bishop1", "C8");
        startingPositions.put("Black Bishop2", "F8");
        startingPositions.put("Black Knight1", "B8");
        startingPositions.put("Black Knight2", "G8");
        startingPositions.put("Black Pawn1", "A7");
        startingPositions.put("Black Pawn2", "B7");
        startingPositions.put("Black Pawn3", "C7");
        startingPositions.put("Black Pawn4", "D7");
        startingPositions.put("Black Pawn5", "E7");
        startingPositions.put("Black Pawn6", "F7");
        startingPositions.put("Black Pawn7", "G7");
        startingPositions.put("Black Pawn8", "H7");
    }

    public static class ChessMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(" ");
            if (parts.length == 3) {
                String color = parts[0];
                String piece = parts[1];
                String position = parts[2];

                // Validate the position is within A1-H8
                if (!isValidPosition(position)) {
                    context.write(new Text("Error"), new Text("Invalid Position: \"" + color + " " + piece + " " + position + "\" - Position must be within A1 to H8."));
                } else {
                    context.write(new Text(color + " " + piece), new Text(position));
                }
            } else {
                context.write(new Text("Error"), new Text("Invalid input format: " + line));
            }
        }

        private boolean isValidPosition(String position) {
            return position.matches("^[A-H][1-8]$");
        }
    }

    public static class ChessReducer extends Reducer<Text, Text, Text, Text> {

        private Set<String> occupiedPositions = new HashSet<>();
        private Set<String> missingPieces = new HashSet<>(startingPositions.keySet());

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String piece = key.toString();
            Set<String> positions = new HashSet<>();

            for (Text value : values) {
                String position = value.toString();
                if (!positions.add(position)) {
                    context.write(new Text("Error"), new Text("Duplicate Position: \"" + piece + " " + position + "\" - Conflicts with another piece."));
                }

                if (!occupiedPositions.add(position)) {
                    context.write(new Text("Error"), new Text("Invalid Position: \"" + piece + " " + position + "\" - Position already occupied by another piece."));
                }

                // Check if the piece matches the expected starting position
                if (startingPositions.containsKey(piece) && !startingPositions.get(piece).equals(position)) {
                    context.write(new Text("Error"), new Text("Position Mismatch: \"" + piece + " " + position + "\" - Expected starting position: " + startingPositions.get(piece)));
                }

                missingPieces.remove(piece);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String missingPiece : missingPieces) {
                context.write(new Text("Missing Piece"), new Text(missingPiece));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Chess Piece Validation");

        job.setJarByClass(ChessPieceValidation.class);
        job.setMapperClass(ChessMapper.class);
        job.setReducerClass(ChessReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

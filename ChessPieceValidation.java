import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    private static final Map<String, Integer> pieceCounts = new HashMap<>();  // To count pieces (e.g., Pawn1, Pawn2)

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

                // Get numbered piece (e.g., White Pawn1, White Pawn2, etc.)
                String numberedPiece = getNumberedPiece(color, piece);

                // Validate the position is within A1-H8
                if (!isValidPosition(position)) {
                    context.write(new Text("Error"), new Text("Invalid Position: \"" + numberedPiece + " " + position + "\" - Position must be within A1 to H8."));
                } else {
                    context.write(new Text(numberedPiece), new Text(position));
                }
            } else {
                context.write(new Text("Error"), new Text("Invalid input format: " + line));
            }
        }

        // Helper method to number pieces
        private String getNumberedPiece(String color, String piece) {
            String key = color + " " + piece;
            int count = pieceCounts.getOrDefault(key, 0) + 1;
            pieceCounts.put(key, count);

            // Only number pieces that have multiples (like Pawns, Rooks, etc.)
            if (piece.equals("Pawn") || piece.equals("Rook") || piece.equals("Bishop") || piece.equals("Knight")) {
                return key + count;  // e.g., White Pawn1, White Pawn2
            }
            return key;  // No numbering for King and Queen
        }

        // Helper method to validate positions
        private boolean isValidPosition(String position) {
            return position.matches("^[A-H][1-8]$");
        }
    }

    public static class ChessReducer extends Reducer<Text, Text, Text, Text> {

        private Set<String> occupiedPositions = new HashSet<>();
        private Set<String> missingPieces = new HashSet<>(startingPositions.keySet());
        private List<String> validPositions = new ArrayList<>();
        private List<String> errorMessages = new ArrayList<>();
        private List<String> whiteMissingPieces = new ArrayList<>();
        private List<String> blackMissingPieces = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String piece = key.toString();
            Set<String> positions = new HashSet<>();
            boolean isValid = true;

            for (Text value : values) {
                String position = value.toString();

                // Check if the piece is in its expected starting position
                if (startingPositions.containsKey(piece) && !startingPositions.get(piece).equals(position)) {
                    errorMessages.add("Position Mismatch: \"" + piece + " " + position + "\" - Expected starting position: " + startingPositions.get(piece));
                    isValid = false;
                    // Track missing positions based on color
                    addMissingPiece(piece);
                } else {
                    missingPieces.remove(piece);  // Remove from missing if correct position
                }

                // Check for duplicate positions
                if (!positions.add(position)) {
                    errorMessages.add("Duplicate Position: \"" + piece + " " + position + "\" - Conflicts with another piece.");
                    isValid = false;
                }

                // Check if position is already occupied by another piece
                if (!occupiedPositions.add(position)) {
                    errorMessages.add("Invalid Position: \"" + piece + " " + position + "\" - Position already occupied by another piece.");
                    isValid = false;
                }
            }

            // If no issues were found, the position is valid
            if (isValid && !positions.isEmpty()) {
                for (String pos : positions) {
                    validPositions.add(piece + " (" + pos + ")");
                }
            }
        }

        private void addMissingPiece(String piece) {
            if (piece.startsWith("White")) {
                whiteMissingPieces.add(piece);
            } else {
                blackMissingPieces.add(piece);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output valid positions in the grouped format
            context.write(new Text("Position Validation:"), null);
            for (String valid : validPositions) {
                context.write(null, new Text("- " + valid));
            }

            // Output missing pieces in the correct format
            if (!whiteMissingPieces.isEmpty() || !blackMissingPieces.isEmpty()) {
                context.write(new Text("Missing Pieces:"), null);
                if (!whiteMissingPieces.isEmpty()) {
                    context.write(null, new Text("- White: " + String.join(", ", whiteMissingPieces)));
                }
                if (!blackMissingPieces.isEmpty()) {
                    context.write(null, new Text("- Black: " + String.join(", ", blackMissingPieces)));
                }
            }

            // Output errors detected
            if (!errorMessages.isEmpty()) {
                context.write(new Text("Errors Detected:"), null);
                for (String error : errorMessages) {
                    context.write(null, new Text("- " + error));
                }
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

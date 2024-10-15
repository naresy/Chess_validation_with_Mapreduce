import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class ChessPieceValidator {

    // Valid positions for White and Black pieces
    private static final String[] WHITE_PAWNS = {"A2", "B2", "C2", "D2", "E2", "F2", "G2", "H2"};
    private static final String[] WHITE_ROOKS = {"A1", "H1"};
    private static final String[] WHITE_KNIGHTS = {"B1", "G1"};
    private static final String[] WHITE_BISHOPS = {"C1", "F1"};
    private static final String WHITE_QUEEN = "D1";
    private static final String WHITE_KING = "E1";

    private static final String[] BLACK_PAWNS = {"A7", "B7", "C7", "D7", "E7", "F7", "G7", "H7"};
    private static final String[] BLACK_ROOKS = {"A8", "H8"};
    private static final String[] BLACK_KNIGHTS = {"B8", "G8"};
    private static final String[] BLACK_BISHOPS = {"C8", "F8"};
    private static final String BLACK_QUEEN = "D8";
    private static final String BLACK_KING = "E8";

    public static class ChessMapper extends Mapper<Object, Text, Text, Text> {
        private HashSet<String> validPositions = new HashSet<>();
        private HashSet<String> occupiedPositions = new HashSet<>();
        private HashMap<String, String> errorLog = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Fill valid positions for White pieces
            for (String pos : WHITE_PAWNS) validPositions.add("White Pawn " + pos);
            for (String pos : WHITE_ROOKS) validPositions.add("White Rook " + pos);
            for (String pos : WHITE_KNIGHTS) validPositions.add("White Knight " + pos);
            for (String pos : WHITE_BISHOPS) validPositions.add("White Bishop " + pos);
            validPositions.add("White Queen " + WHITE_QUEEN);
            validPositions.add("White King " + WHITE_KING);

            // Fill valid positions for Black pieces
            for (String pos : BLACK_PAWNS) validPositions.add("Black Pawn " + pos);
            for (String pos : BLACK_ROOKS) validPositions.add("Black Rook " + pos);
            for (String pos : BLACK_KNIGHTS) validPositions.add("Black Knight " + pos);
            for (String pos : BLACK_BISHOPS) validPositions.add("Black Bishop " + pos);
            validPositions.add("Black Queen " + BLACK_QUEEN);
            validPositions.add("Black King " + BLACK_KING);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(" ");
            if (parts.length != 3) {
                errorLog.put(line, "Invalid entry format. Expected format: <Color> <PieceType> <Position>");
                return;
            }

            String color = parts[0];
            String type = parts[1];
            String position = parts[2];

            // Check if the position is valid
            if (!isValidPosition(position)) {
                errorLog.put(line, "Invalid Position: " + position);
                return;
            }

            String pieceKey = color + " " + type + " " + position;

            // Check for duplicate positions
            if (occupiedPositions.contains(position)) {
                errorLog.put(line, "Duplicate Position: " + pieceKey + " conflicts with another piece.");
                return;
            }

            // Add to occupied positions
            occupiedPositions.add(position);

            // Check for valid starting positions
            if (!validPositions.contains(pieceKey)) {
                errorLog.put(line, "Mismatched starting position: " + pieceKey);
                return;
            }

            // Log valid piece
            context.write(new Text(pieceKey), new Text("Valid"));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String error : errorLog.keySet()) {
                context.write(new Text("Error"), new Text(error + " - " + errorLog.get(error)));
            }
        }

        private boolean isValidPosition(String position) {
            return position.matches("[A-H][1-8]");
        }
    }

    public static class ChessReducer extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, HashSet<String>> expectedPositions = new HashMap<>();
        private HashSet<String> validPieces = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Initialize expected positions for each piece type
            initializeExpectedPositions();
        }

        private void initializeExpectedPositions() {
            expectedPositions.put("White Pawn", new HashSet<>(Arrays.asList("A2", "B2", "C2", "D2", "E2", "F2", "G2", "H2")));
            expectedPositions.put("White Rook", new HashSet<>(Arrays.asList("A1", "H1")));
            expectedPositions.put("White Knight", new HashSet<>(Arrays.asList("B1", "G1")));
            expectedPositions.put("White Bishop", new HashSet<>(Arrays.asList("C1", "F1")));
            expectedPositions.put("White Queen", new HashSet<>(Arrays.asList("D1")));
            expectedPositions.put("White King", new HashSet<>(Arrays.asList("E1")));

            expectedPositions.put("Black Pawn", new HashSet<>(Arrays.asList("A7", "B7", "C7", "D7", "E7", "F7", "G7", "H7")));
            expectedPositions.put("Black Rook", new HashSet<>(Arrays.asList("A8", "H8")));
            expectedPositions.put("Black Knight", new HashSet<>(Arrays.asList("B8", "G8")));
            expectedPositions.put("Black Bishop", new HashSet<>(Arrays.asList("C8", "F8")));
            expectedPositions.put("Black Queen", new HashSet<>(Arrays.asList("D8")));
            expectedPositions.put("Black King", new HashSet<>(Arrays.asList("E8")));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Handle the "Error" key separately
            if (key.toString().equals("Error")) {
                for (Text value : values) {
                    context.write(key, value); // Log the error messages
                }
                return;
            }

            String[] parts = key.toString().split(" ");

            // Ensure there are at least 3 parts: color, piece type, and position
            if (parts.length < 3) {
                context.write(new Text("Error"), new Text("Invalid format in key: " + key.toString()));
                return;  // Skip this record if the format is invalid
            }

            String color = parts[0];
            String pieceType = parts[1];
            String position = parts[2];

            String pieceKey = color + " " + pieceType;

            // Remove found positions from the expected positions
            if (expectedPositions.containsKey(pieceKey)) {
                expectedPositions.get(pieceKey).remove(position);
            }

            validPieces.add(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output valid positions
            context.write(new Text("Valid Positions:"), new Text(""));
            for (String piece : validPieces) {
                context.write(new Text(piece), new Text("Valid"));
            }

            // Output missing pieces with positions
            context.write(new Text("Missing Pieces:"), new Text(""));
            for (String pieceType : expectedPositions.keySet()) {
                HashSet<String> missingPositions = expectedPositions.get(pieceType);
                if (!missingPositions.isEmpty()) {
                    String[] colorAndType = pieceType.split(" ");

                    // Check if colorAndType has at least two elements
                    if (colorAndType.length < 2) {
                        context.write(new Text("Error"), new Text("Invalid pieceType format: " + pieceType));
                        continue;  // Skip this entry if it's malformed
                    }

                    String color = colorAndType[0];
                    String type = colorAndType[1];

                    for (String position : missingPositions) {
                        context.write(new Text("- " + color + ": 1 " + type), new Text("(" + position + ")"));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ChessPieceValidator <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Chess Piece Validation");
        job.setJarByClass(ChessPieceValidator.class);
        job.setMapperClass(ChessMapper.class);
        job.setReducerClass(ChessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

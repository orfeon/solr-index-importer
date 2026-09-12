package net.orfeon.solr.importer.cli;

import java.util.Arrays;

public final class Command {

    private static final String USAGE = """
            usage: <command> [args...]
              importAvro     <coreName> <source>   build the core's index from Avro files
              generateSchema <source> [coreName]   print a schema.xml derived from the Avro schema of <source>
            <source> is a local file, a local directory, or gs://bucket/prefix
            """;

    private Command() {
    }

    public static void main(final String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("command is required\n" + USAGE);
        }

        final String cmd = args[0];
        final String[] cmdArgs = Arrays.copyOfRange(args, 1, args.length);
        System.out.println("cmd: " + cmd + " with args: " + Arrays.asList(cmdArgs));

        switch (cmd) {
            case "importAvro" -> AvroImport.main(cmdArgs);
            case "generateSchema" -> GenerateSchema.main(cmdArgs);
            default -> throw new IllegalArgumentException("unknown command: " + cmd + "\n" + USAGE);
        }
    }

}

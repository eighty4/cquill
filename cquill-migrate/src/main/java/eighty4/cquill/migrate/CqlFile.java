package eighty4.cquill.migrate;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class CqlFile {

    private final String name;

    private final String absolutePath;

    CqlFile(File file) {
        name = file.getName();
        absolutePath = file.getAbsolutePath();
    }

    int getVersion() {
        return Integer.parseInt(name.substring(1, 4));
    }

    List<String> cqlLines() {
        try {
            String[] cqlLines = Files.readString(Paths.get(absolutePath)).split(";");
            return Stream.of(cqlLines).filter(cql -> !cql.matches("\\s*")).map(String::trim).collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalStateException("error reading cql file " + name, e);
        }
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }
}

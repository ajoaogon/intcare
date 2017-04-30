package pt.chp.intcare.datamgmt.producer.utils;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtils {


    public static File[] listFiles(String path) throws Exception {

        return new File(path).listFiles();
    }

    public static List<String> readFile(File file) throws Exception {

        return Files.lines(file.toPath())
                .collect(Collectors.toList());
    }
}

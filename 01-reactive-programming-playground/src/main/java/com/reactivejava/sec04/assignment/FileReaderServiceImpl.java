package com.reactivejava.sec04.assignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class FileReaderServiceImpl implements FileReaderService {

    private static final Logger log = LoggerFactory.getLogger(FileReaderServiceImpl.class);

    @Override
    public Flux<String> read(Path path) {
       return Flux.generate(
               ()-> openFile(path),
               this::readFile,
               this::closeFile
       );
    }

    private void closeFile(BufferedReader reader) {
       try {
           reader.close();
       }
       catch (IOException e) {
           log.error(e.getMessage());
       }
    }

    private BufferedReader readFile(BufferedReader reader, SynchronousSink<String> stringSynchronousSink) {
        try{
            String s = reader.readLine();
            if(Objects.isNull(s)){
                stringSynchronousSink.complete();
            }
            else {
                stringSynchronousSink.next(s);
            }
        }
        catch (Exception e){
        }
        return reader;
    }

    private BufferedReader openFile(Path path) throws IOException {
        log.info("Opening file");
        return Files.newBufferedReader(path);
    }


}

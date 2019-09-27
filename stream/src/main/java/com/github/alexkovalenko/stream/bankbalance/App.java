package com.github.alexkovalenko.stream.bankbalance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class App {
    public static void main(String[] args) throws IOException {
        Set<String> ids = Files.lines(Paths.get("1"))
                .flatMap(line -> Stream.of(line.split(",")))
                .map(String::strip)
                .distinct()
                .collect(toSet());
        System.out.println(ids.size());

    }
}

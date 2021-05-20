package com.example;

import java.util.Arrays;

public enum FileCSVEnum {

    CHARACTER_DEFAULT(";"),
    PATTERN_DATE_DEFAULT("^(3[01]|[12][0-9]|0[1-9])/(1[0-2]|0[1-9])/[0-9]{4}$"),
    PATTERN_SIMPLE_DATE_FORMAT("dd/MM/yyyy"),
    STATUS_IN_PROCESS_FILE("PROCESSING"),
    STATUS_FINISH_FILE("FINISHED");

    private final String id;

    private FileCSVEnum(String id) {
        this.id = id;
    }

    public static String valueFromId(String id) {
        return (String) Arrays.stream(values()).filter((dse) -> {
            return dse.getId().contentEquals(id);
        }).map(Enum::name).findFirst().orElseThrow(() -> {
            return new RuntimeException("FILE_CSV_DEFAULT_ENUM_NOT_VALID");
        });
    }

    public String getId() {
        return this.id;
    }

}


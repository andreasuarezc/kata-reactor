package com.example;
import java.util.Arrays;


public enum ExperienceEnum {
    ACTIVE("38c1a58d-d435-4bf1-a1ba-0c82d5dc7f16"),
    DISABLED("969e0631-2121-4965-aa4e-9779d5d6fab2"),
    PENDING("a8d259b7-b358-4045-a65f-57a045e7cf98"),
    DRAFT("eaea43e4-4753-4637-b8f4-36cfa1f9f479");

    private final String id;

    private ExperienceEnum(String id) {
        this.id = id;
    }

    public static String nameFromId(String id) {
        return (String)Arrays.stream(values()).filter((dse) -> {
            return dse.getId().contentEquals(id);
        }).map(Enum::name).findFirst().orElseThrow(() -> {
            return new RuntimeException("OBJECT_STATUS_NOT_VALID");
        });
    }

    public static String idFromName(String name) {
        return (String) Arrays.stream(values()).filter((dse) -> {
            return dse.name().contentEquals(name);
        }).map(value -> value.getId()).findFirst().orElseThrow(() -> {
            return new RuntimeException("OBJECT_NAME_NOT_VALID");
        });
    }

    public String getId() {
        return this.id;
    }
}


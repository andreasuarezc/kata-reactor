package com.example;


public enum ValidateCouponEnum {

    EAN_13("ean_13"),
    EAN_39("ean_39"),
    ALPHANUMERIC("alphanumeric");

    private final String typeOfEnum;

    ValidateCouponEnum(String typeOfEnum) {
        this.typeOfEnum = typeOfEnum;
    }

    public String getTypeOfEnum() {
        return typeOfEnum;
    }
}



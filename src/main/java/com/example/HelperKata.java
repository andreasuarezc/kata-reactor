package com.example;


import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


public class HelperKata {
    private static final  String EMPTY_STRING = "";
    private static String anteriorBono = null;

    private HelperKata() {
    }

    public static Flux<CouponDetailDto> getListFromBase64File(final String fileBase64) {
        AtomicInteger counter = new AtomicInteger(0);
        Set<String> codes = new HashSet<>();
        return          createFluxFrom(fileBase64)
                        .map(line -> getTupleOfLine(line, line.split(FileCSVEnum.CHARACTER_DEFAULT.getId()), FileCSVEnum.CHARACTER_DEFAULT.getId()))
                        .map(tuple ->
                            getCouponDetailDto(counter, codes, tuple));
    }

    private static CouponDetailDto getCouponDetailDto(AtomicInteger counter, Set<String> codes,  Tuple2<String, String> tuple) {
        String dateValidated = null;
        String errorMessage = null;

        if (tuple.getT1().isBlank() || tuple.getT2().isBlank()) {
            errorMessage = ExperienceErrorsEnum.FILE_ERROR_COLUMN_EMPTY.toString();
        } else if (!codes.add(tuple.getT1())) {
            errorMessage = ExperienceErrorsEnum.FILE_ERROR_CODE_DUPLICATE.toString();
        } else if (!validateDateRegex(tuple.getT2())) {
            errorMessage = ExperienceErrorsEnum.FILE_ERROR_DATE_PARSE.toString();
        } else if (validateDateIsMinor(tuple.getT2())) {
            errorMessage = ExperienceErrorsEnum.FILE_DATE_IS_MINOR_OR_EQUALS.toString();
        } else
            dateValidated = tuple.getT2();
        return getCouponDetailDto(counter, tuple, dateValidated, errorMessage);
    }

    private static CouponDetailDto getCouponDetailDto(AtomicInteger counter, Tuple2<String, String> tuple, String dateValidated, String errorMessage) {
        return CouponDetailDto.aCouponDetailDto()
                .withCode(anteriorBono == null || anteriorBono.equals("") ? bonoVacioONullo(tuple.getT1()) : bonoAnteriorIgualODiferenteAlEnviado(tuple.getT1()))
                .withDueDate(dateValidated)
                .withNumberLine(counter.incrementAndGet())
                .withMessageError(errorMessage)
                .withTotalLinesFile(1)
                .build();
    }

    private static String bonoVacioONullo(String bonoEnviado){
        anteriorBono = typeBono(bonoEnviado);
        return  (anteriorBono.equals("")) ? null : bonoEnviado;
    }

    private static String bonoAnteriorIgualODiferenteAlEnviado(String bonoEnviado){
        return (anteriorBono.equals(typeBono(bonoEnviado))) ? bonoEnviado : null;
    }

    private static Flux<String> createFluxFrom(String fileBase64) {
        return Flux.using(
                () -> new BufferedReader(new InputStreamReader(
                        new ByteArrayInputStream(decodeBase64(fileBase64))
                )).lines().skip(1),
                Flux::fromStream,
                Stream::close
        );
    }

    private static String typeBono(String bonoIn) {
        if (bonoCharacterAndLength(bonoIn)){
            return ValidateCouponEnum.EAN_13.getTypeOfEnum();
        }return bonoStartWithAndLength(bonoIn)
                ? ValidateCouponEnum.EAN_39.getTypeOfEnum()
                : ValidateCouponEnum.ALPHANUMERIC.getTypeOfEnum();
    }

    private static boolean bonoCharacterAndLength(String bonoIn){
        return bonoIn.chars().allMatch(Character::isDigit) && bonoLengthHigher(bonoIn.length(), 12)
                && bonoLengthSmaller(bonoIn.length(), 13);
    }
    
    private static boolean bonoStartWithAndLength(String bonoIn){
        return bonoIn.startsWith("*") && bonoLengthHigher(bonoIn.replace("*", "").length(),1)
                 && bonoLengthSmaller(bonoIn.replace("*", "").length(), 43);
    }

    private static boolean bonoLengthHigher(int bonoInLength, int number){
        return (bonoInLength >= number);
    }

    private static boolean bonoLengthSmaller(int bonoInLength, int number){
        return (bonoInLength <= number);
    }
    
    private static boolean validateDateRegex(String dateForValidate) {
        String regex = FileCSVEnum.PATTERN_DATE_DEFAULT.getId();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(dateForValidate);
        return matcher.matches();
    }

    private static byte[] decodeBase64(final String fileBase64) {
        return Base64.getDecoder().decode(fileBase64);
    }

    private static Tuple2<String, String> getTupleOfLine(String line, String[] array, String characterSeparated) {
        if (arrayCheck(array)){
            return Tuples.of(EMPTY_STRING, EMPTY_STRING);
        } else if (lengthArray(array , 2)){
            if(lineStartWith(characterSeparated, line)){
                return Tuples.of(EMPTY_STRING, array[0]);
            }return Tuples.of(array[0], EMPTY_STRING);
        }return Tuples.of(array[0], array[1]);
    }

    private static boolean arrayCheck(String[] array){
        return (Objects.isNull(array)) || (array.length == 0);
    }
    
    private static boolean lengthArray(String[] array, int number){
        return array.length < number;
    }
    
    private static boolean lineStartWith(String characterSeparated, String line){
        return line.startsWith(characterSeparated);
    }

    public static boolean validateDateIsMinor(String dateForValidate) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(FileCSVEnum.PATTERN_SIMPLE_DATE_FORMAT.getId());
            Date dateActual = sdf.parse(sdf.format(new Date()));
            Date dateCompare = sdf.parse(dateForValidate);
            return ((dateCompare.compareTo(dateActual)) <= 0);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}

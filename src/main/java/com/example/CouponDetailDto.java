package com.example;

import java.util.Objects;

public class CouponDetailDto {
    private String code;
    private String dueDate;
    private int numberLine;
    private String messageError;
    private int totalLinesFile;


    private CouponDetailDto() {
    }

    public static CouponDetailDto aCouponDetailDto() {
        return new CouponDetailDto();
    }

    public CouponDetailDto withCode(String code) {
        this.code = code;
        return this;
    }

    public CouponDetailDto withDueDate(String dueDate) {
        this.dueDate = dueDate;
        return this;
    }

    public CouponDetailDto withNumberLine(int numberLine) {
        this.numberLine = numberLine;
        return this;
    }

    public CouponDetailDto withMessageError(String messageError) {
        this.messageError = messageError;
        return this;
    }

    public CouponDetailDto withTotalLinesFile(int totalLinesFile) {
        this.totalLinesFile = totalLinesFile;
        return this;
    }

    public CouponDetailDto build() {
        CouponDetailDto couponDetailDto = new CouponDetailDto();
        couponDetailDto.numberLine = this.numberLine;
        couponDetailDto.totalLinesFile = this.totalLinesFile;
        couponDetailDto.messageError = this.messageError;
        couponDetailDto.dueDate = this.dueDate;
        couponDetailDto.code = this.code;
        return couponDetailDto;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CouponDetailDto that = (CouponDetailDto) o;
        return numberLine == that.numberLine &&
                totalLinesFile == that.totalLinesFile &&
                Objects.equals(code, that.code) &&
                Objects.equals(dueDate, that.dueDate) &&
                Objects.equals(messageError, that.messageError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, dueDate, numberLine, messageError, totalLinesFile);
    }

    @Override
    public String toString() {
        return "CouponDetailDto{" +
                "code='" + code + '\'' +
                ", dueDate='" + dueDate + '\'' +
                ", numberLine=" + numberLine +
                ", messageError='" + messageError + '\'' +
                ", totalLinesFile=" + totalLinesFile +
                '}';
    }
}

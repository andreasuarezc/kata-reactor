package com.example;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class HelperKataTest {
    String file = "Q09ESUdPO0ZFQ0hBX1ZFTkNJQ0lNCjExMTE7MDEvMDEvMjAxMQozMzMzOzE4LzA5LzIwCjQ0NDQ7MTgvMDkvMjAKMTExMTsxMy8wNy8yMDExCjExMTE7MTMvMTIvMjAyMgoxMTExOzE4LzEzLzIwMjIKMjIyMjsxOC8wOS8yMAo";
    @Test
    void exampleExecutor(){

        StepVerifier
                .create(HelperKata.getListFromBase64File(file))
                .expectNext( CouponDetailDto.aCouponDetailDto()
                        .withCode("1111")
                        .withDueDate(null)
                        .withNumberLine(1)
                        .withMessageError("FILE_DATE_IS_MINOR_OR_EQUALS")
                        .withTotalLinesFile(1)
                        .build())
                .expectNext( CouponDetailDto.aCouponDetailDto()
                        .withCode("3333")
                        .withDueDate(null)
                        .withNumberLine(2)
                        .withMessageError("FILE_ERROR_DATE_PARSE")
                        .withTotalLinesFile(1)
                        .build())
                .expectNext(CouponDetailDto.aCouponDetailDto()
                        .withCode("4444")
                        .withDueDate(null)
                        .withNumberLine(3)
                        .withMessageError("FILE_ERROR_DATE_PARSE")
                        .withTotalLinesFile(1)
                        .build())
                .expectNext( CouponDetailDto.aCouponDetailDto()
                        .withCode("1111")
                        .withDueDate(null)
                        .withNumberLine(4)
                        .withMessageError("FILE_ERROR_CODE_DUPLICATE")
                        .withTotalLinesFile(1)
                        .build())
                .expectNext( CouponDetailDto.aCouponDetailDto()
                        .withCode("1111")
                        .withDueDate(null)
                        .withNumberLine(5)
                        .withMessageError("FILE_ERROR_CODE_DUPLICATE")
                        .withTotalLinesFile(1)
                        .build())
                .expectNext( CouponDetailDto.aCouponDetailDto()
                        .withCode("1111")
                        .withDueDate(null)
                        .withNumberLine(6)
                        .withMessageError("FILE_ERROR_CODE_DUPLICATE")
                        .withTotalLinesFile(1)
                        .build())
                .expectNext( CouponDetailDto.aCouponDetailDto()
                        .withCode("2222")
                        .withDueDate(null)
                        .withNumberLine(7)
                        .withMessageError("FILE_ERROR_DATE_PARSE")
                        .withTotalLinesFile(1)
                        .build())
                .verifyComplete();

    }
}
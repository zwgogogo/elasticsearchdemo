package com.example.esdemo.annotation;

import java.lang.annotation.*;

/**
 * @author P52
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DealEsIndex {

    /**
     * document信息
     * @return
     */
    String dealDocumentParam() default "";
}

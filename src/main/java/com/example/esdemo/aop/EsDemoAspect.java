package com.example.esdemo.aop;

import com.example.esdemo.annotation.DealEsIndex;
import com.example.esdemo.constant.FullTextSearchConstant;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

@Component
@Aspect
public class EsDemoAspect {

    @Pointcut("@annotation(com.example.esdemo.annotation.DealEsIndex)")
    public void EsDemoPoint() {
    }

    @After(value = "EsDemoPoint()&&@annotation(dealESIndex)", argNames = "joinPoint,dealESIndex")
    public void dealIndex(JoinPoint joinPoint, DealEsIndex dealESIndex) {
        String dealType = dealESIndex.dealDocumentParam();
        switch (dealType) {
            case FullTextSearchConstant.ADD_INDEX:
                break;
            case FullTextSearchConstant.DELETE_INDEX:
                break;
            default:
                break;
        }
    }
}

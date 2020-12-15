package com.example.esdemo.controller;

import com.example.esdemo.service.EsDemoService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/esDemoController")
public class EsDemoController {

    /**
     * ES服务注入
     */
    private EsDemoService esDemoService;


}

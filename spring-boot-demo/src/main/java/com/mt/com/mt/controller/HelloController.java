package com.mt.com.mt.controller;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/hello")
public class HelloController {

    @RequestMapping(value = "/",method = RequestMethod.GET,produces = MediaType.APPLICATION_JSON_VALUE)
    public String getDemo(){
        return "Welcome to the spring boot hello - world application chang  ";
    }
}

package com.seven.mq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * @Description: TODO
 * @Author 参考文档 https://www.cnblogs.com/xuwc/p/9034352.html
 * @Date 2020/4/14 17:30
 * @Version V1.0
 **/
@SpringBootApplication
@ComponentScan({"com.seven.mq"})
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}

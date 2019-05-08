package com.jangni.zookeeper;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(value = "com.jangni.zookeeper")
public class ZookeeperApplication {

	public static void main(String[] args) {

		ConfigurableApplicationContext context = SpringApplication.run(ZookeeperApplication.class, args);
		Test test = context.getBean("test", Test.class);
		System.out.println(test.toString());
	}


}

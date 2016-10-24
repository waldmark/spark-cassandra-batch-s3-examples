package com.objectpartners.standalone;

import com.objectpartners.config.SparkCassandraConfig;
import com.objectpartners.spark.SparkCassandraRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class MainApplication {

    public static void main(String[] args) {
        ApplicationContext ctx;
        SparkCassandraRunner runner;

        ctx = new AnnotationConfigApplicationContext(SparkCassandraConfig.class);
        runner = (SparkCassandraRunner) ctx.getBean("sparkCassandraRunner");

        if(null != runner) {
            runner.runSparkStreamProcessing();
        }

    }
}


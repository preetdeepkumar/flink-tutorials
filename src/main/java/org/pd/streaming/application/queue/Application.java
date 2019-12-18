package org.pd.streaming.application.queue;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableJms
@EnableScheduling
public class Application 
{
	private static final String LOCAL_ACTIVE_MQ_URL = "vm://localhost?broker.persistent=false";

    public static void main(String[] args) 
	{
		SpringApplication.run(Application.class, args);
	}
	
	@Bean
	public Session mySession() throws JMSException
	{
	    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(LOCAL_ACTIVE_MQ_URL);
	    Connection connection = factory.createConnection();
	    connection.start();
	    return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	}
}

package com.yonic.demo.rabbitmq;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;

public class ProcessMessagesAsBatch {
	
	private static final String QUEUE_NAME = "dev_queue_1";
	private static final String HOSTNAME = "127..0.0.1";
	private static final Integer PORT = 5671;
	private static final String USERNAME = "developer";
	private static final String PASSWORD = "dev#user123789";
	private static final String VIRTUALHOST = "/";
	private static final Boolean SSL_ENABLED = Boolean.TRUE;
	private static final Integer BATCH_SIZE = 10;

	public static void main(String args[]) {
		ConnectionFactory factory = new ConnectionFactory();

		Connection connection = null;
		
		try {
			factory.setHost(HOSTNAME);
			factory.setPort(PORT);
			factory.setUsername(USERNAME);
			factory.setPassword(PASSWORD);
			factory.setVirtualHost(VIRTUALHOST);
			if(SSL_ENABLED) {
				factory.useSslProtocol();
			}
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			Consumer batchConsumer = new BatchConsumer().getBatchConsumer(channel, QUEUE_NAME);
			channel.basicQos(BATCH_SIZE);
			String consumerTag = channel.basicConsume(QUEUE_NAME, false, batchConsumer);
			channel.basicCancel(consumerTag);
			channel.close();
			connection.close();
		} catch (IOException | TimeoutException e) {
			System.out.println("Error in rabbit mq connection " + e);
		} catch (KeyManagementException | NoSuchAlgorithmException e) {
			System.out.println("Error in ssl connection " + e);
		} catch (Exception e) {
			System.out.println("Error in processing " + e);
		}

	}

}

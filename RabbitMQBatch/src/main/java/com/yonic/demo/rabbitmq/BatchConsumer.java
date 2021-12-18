package com.yonic.demo.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class BatchConsumer {

	public Consumer getBatchConsumer(Channel channel, String queueName) {
		
		Consumer batchConsumer = new DefaultConsumer(channel) {

			Envelope lastMessageEnvelope;
			
			private Envelope getLastMessageEnvelope() {
				return lastMessageEnvelope;
			}

			private void setLastMessageEnvelope(Envelope lastMessageEnvelope) {
				this.lastMessageEnvelope = lastMessageEnvelope;
			}

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				System.out.println("Read message : " + (new String(body)));
				setLastMessageEnvelope(envelope);
			}

			@Override
			public void handleCancelOk(String consumerTag) {
				try {
					if(lastMessageEnvelope != null) {
						channel.basicAck(getLastMessageEnvelope().getDeliveryTag(), true);
					} else {
						System.out.println("No messages are avaiable in queue : " + queueName);
					}
				} catch (IOException e) {
					System.out.println("Error in sending ACK " + e);
				}
			}
		};
		
		return batchConsumer;
	}
	
}

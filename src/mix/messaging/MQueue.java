package mix.messaging;

import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.Collections;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import mix.model.ISerializable;
import mix.model.bank.BankInterestReply;
import mix.model.bank.BankInterestRequest;
import mix.model.loan.LoanReply;
import mix.model.loan.LoanRequest;

public class MQueue {
    private Connection connection = null;
    private Session session = null;
    private MessageProducer messageProducer = null;
    private MessageConsumer messageConsumer = null;

    private Queue loanRequestQueue = null;
    private Queue loanReplyQueue = null;
    private Queue bankInterestReplyQueue = null;
    private Queue bankInterestRequestQueue = null;

    private void startConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        factory.setTrustedPackages(Collections.singletonList("mix.model"));// set all models as trusted
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // We need different queue/channels for each direction of communication.
        loanRequestQueue = session.createQueue("loanRequestQueue");
        loanReplyQueue = session.createQueue("loanReplyQueue");
        bankInterestRequestQueue = session.createQueue("bankInterestRequestQueue");
        bankInterestReplyQueue = session.createQueue("bankInterestReplyQueue");

    }


    /**
     * Objects which are serializable are sent to the proper que
     * Todo: scalability is not on point for the queue's
     *
     * @param obj object which you want to send.
     */
    public void sendMessage(ISerializable obj) {
        try {
            if (connection == null) {
                startConnection();
            }

            if (obj instanceof LoanRequest) {
                messageProducer = session.createProducer(loanRequestQueue);
            } else if (obj instanceof LoanReply) {
                messageProducer = session.createProducer(loanReplyQueue);
            } else if (obj instanceof BankInterestRequest) {
                messageProducer = session.createProducer(bankInterestRequestQueue);
            } else if (obj instanceof BankInterestReply) {
                messageProducer = session.createProducer(bankInterestReplyQueue);
            }

            ObjectMessage message = session.createObjectMessage();
            message.setObject(obj);
            messageProducer.send(message);

        } catch (Exception e) {
            System.out.println(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    System.out.println(e);
                }
            }
        }


    }

    public void listen(String queueName, MessageListener listener) {
        try {
            if (connection == null) {
                startConnection();
            }

            //Todo: scalability
            if (queueName.equals("loanRequestQueue")) {
                messageConsumer = session.createConsumer(loanRequestQueue);
            } else if (queueName.equals("loanReplyQueue")) {
                messageConsumer = session.createConsumer(loanReplyQueue);
            } else if (queueName.equals("bankInterestRequestQueue")) {
                messageConsumer = session.createConsumer(bankInterestRequestQueue);
            } else if (queueName.equals("bankInterestReplyQueue")) {
                messageConsumer = session.createConsumer(bankInterestReplyQueue);
            }

            messageConsumer.setMessageListener(listener);
            connection.start();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}

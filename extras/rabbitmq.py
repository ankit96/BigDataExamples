#producer
#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello') #create queue if not exists 

channel.basic_publish(exchange='', routing_key='hello', body='Hello World!') #default exchange = '' which is a direct exchange , routing_key = queue name 
print(" [x] Sent 'Hello World!'")
connection.close()



#consumer
#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


channel.basic_consume(
    queue='hello', on_message_callback=callback, auto_ack=True) #by default auto_ack = false i.e manual ack enabled which require us to implement ack code 

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()



#---------------------------------------------------------------------------------------------------


import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def main(msg: func.ServiceBusMessage):

    # temp = msg.get_body()
    temp = msg.get_body().decode('utf-8')
    notification_id = int(temp)
    logging.info(
        'Python ServiceBus queue trigger processed message: %s', notification_id)

    # TODO: Get connection to database
    conn = psycopg2.conn = psycopg2.connect(
        dbname="techconfdb", user="harisu@techconfdbserver", password="Pa55w0rd/", host="techconfdbserver.postgres.database.azure.com")
    cur = conn.cursor()
    try:
        # TODO: Get notification message and subject fron database using the notification_id
        cur.execute("SELECT message, subject FROM notification where id = %s;", (notification_id,))
        notification = cur.fetchone()
        logging.info('ferched notification and it value is: %s',notification)


        # TODO: Get attendees email and name
        cur.execute("SELECT email, first_name, last_name FROM attendee;")
        attendees = cur.fetchall()
        logging.info('ferched attendees and it value is: %s', attendees)

        # TODO: Loop thru each attendee and send an email with a personalized subject
        for attendee in attendees:
            subject = '{}: {}'.format(attendee[1], notification[1])
            # send_email(attendee[0], subject, notification[0])

        status = 'Notified {} attendees'.format(len(attendees))
        logging.info(" shit happens")
        query = """Update notification set status = %s, completed_date = %s where id = %s"""
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        cur.execute(query,(status, datetime.utcnow(), notification_id))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        logging.info('hello');
        # TODO: Close connection
        cur.close()
        conn.close()


def send_email(email, subject, body):
    message = Mail(
        from_email='info@techconf.com',
        to_emails=email,
        subject=subject,
        plain_text_content=body)
    SENDGRID_API_KEY = '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"'
    sg = SendGridAPIClient(SENDGRID_API_KEY)    
    sg.send(message)


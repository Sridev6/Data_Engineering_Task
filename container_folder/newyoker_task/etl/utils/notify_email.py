import traceback
from smtplib import SMTP_SSL as SMTP, SMTPHeloError, SMTPAuthenticationError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

"""Task Modules"""
from etl.utils import DEFAULT_EMAIL_ADDRESS, DEFAULT_PASSWORD, \
    DEFAULT_SUBJECT



def send_mail(receivers, message, task_name):
    print("SENDING MAIL : " + ','.join(receivers))
    msg = MIMEMultipart()
    msg['From'] = DEFAULT_EMAIL_ADDRESS
    msg['Subject'] = DEFAULT_SUBJECT + task_name.upper()
    message = message
    msg.attach(MIMEText(message))

    ServerConnect = False
    smtp_server = None
    try:
        smtp_server = SMTP('smtp.gmail.com','465')
        smtp_server.login(DEFAULT_EMAIL_ADDRESS, DEFAULT_PASSWORD)
        ServerConnect = True
    except SMTPHeloError as e:
        print ("Server did not reply")
    except SMTPAuthenticationError as e:
        print ("Incorrect username/password combination")
    except Exception as e:
        print(e)

    if ServerConnect == True:
        try:
            smtp_server.sendmail(DEFAULT_EMAIL_ADDRESS, receivers, msg.as_string())
            print ("Successfully sent email")
        except Exception as e:
            print ("Error: unable to send email", e)
        finally:
            smtp_server.close()
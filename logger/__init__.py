from credentials import BASE_LOG, EMAIL_USERNAME, EMAIL_PASSWORD, PUSHBULLET_KEY
import string, logging, logging.handlers
import os
import atexit
from datetime import datetime
from pushbullet import Pushbullet

TOADDRS = [
        'dylanzam@gmail.com',
        'dylan.z@yourdatalake.com'
    ]

pb = Pushbullet(PUSHBULLET_KEY)

class BufferingSMTPHandler(logging.handlers.BufferingHandler):
    def __init__(self, mailhost, mailport, fromaddr, toaddrs, subject, capacity):
        logging.handlers.BufferingHandler.__init__(self, capacity)
        self.mailhost = mailhost
        self.mailport = mailport 
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(message)s"))
        atexit.register(self.flush)

    def getSubject(self, record):
        return "["+str(record.levelname)+"] - "+self.subject

    def flush(self):
        if len(self.buffer) > 0:
            try:
                import smtplib
                port = self.mailport
                if not port:
                    port = smtplib.SMTP_PORT
                with smtplib.SMTP(self.mailhost, port) as smtp: 
                    smtp.starttls()
                    smtp.login(self.fromaddr, EMAIL_PASSWORD)
                    msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (self.fromaddr, ','.join(self.toaddrs), self.subject)
                    for record in self.buffer:
                        s = self.format(record)
                        print(s)
                        msg = msg + s + "\r\n"
                    smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            except:
                self.handleError(None)  # no particular record
            self.buffer = []


def mylogger(fn, subject=None):
    if subject is None: subject = ''

    today = datetime.now().strftime('%Y_%m_%d')
    fn = fn[:-4] if fn.endswith('.log') else fn
    fn += f'_{today}.log'

    _log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(os.path.join(BASE_LOG, fn))
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter(_log_format))
    smpt_handler = BufferingSMTPHandler(
        mailhost='smtp.office365.com',
        mailport=587,
        fromaddr=EMAIL_USERNAME,
        toaddrs=TOADDRS,
        subject= subject,
        capacity=100
    )
    
    smpt_handler.setLevel(logging.INFO)
    smpt_handler.setFormatter(logging.Formatter(_log_format))
    logger.addHandler(smpt_handler)
    logger.addHandler(file_handler)
    return logger


if __name__ == '__main__':
    logger = mylogger('test.log', subject='This is my subject')
    logger.info('This is just a test')
    logger.warning('warning!')
    logger.info('This is just another test')

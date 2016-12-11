#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
from sys import argv
from account_info import FROM_EMAIL_HOST, EMAIL_USER, EMAIL_PASSWD

import smtplib
from email.mime.text import MIMEText

reload(sys)
sys.setdefaultencoding("utf-8")

TO_EMAIL = "shuyang790@gmail.com"


class Email:
    def __init__(self):
        self.mail_host = FROM_EMAIL_HOST
        self.mail_user = EMAIL_USER
        self.mail_pass = EMAIL_PASSWD
        self.mail_postfix = FROM_EMAIL_HOST

    def send(self, content, To="", Me="Vroom", Subject=""):
        Me = Me + "<" + self.mail_user + "@" + self.mail_postfix + ">"
        To = TO_EMAIL if To == "" else To
        msg = MIMEText(content, _subtype='plain', _charset='utf-8')
        msg['Subject'] = "[EfT] [%s] " % (time.strftime(
            '%Y-%m-%d %H:%M:%S',
            time.localtime(time.time())
            )) + Subject
        msg['From'] = Me
        msg['To'] = To
        for i in range(0, 3):
            try:
                server = smtplib.SMTP()
                server.connect(self.mail_host)
                server.login(self.mail_user, self.mail_pass)
                server.sendmail(Me, To, msg.as_string())
                server.close()
                print "[Info] Send mail %s" % (msg['Subject'])
                return True
            except Exception, e:
                print "[Error]", str(e)
        return False


if __name__ == "__main__":
    if len(argv) <= 1:
        customSubject = ""
    else:
        customSubject = " | " + ' '.join(argv[1:])
    f = open("output.log", "r")
    fileContent = f.read()
    f.close()
    m = Email()
    m.send(fileContent, Subject="Spark-run" + customSubject)

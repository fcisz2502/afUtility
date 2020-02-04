#!/usr/bin/python
# -*- coding: UTF-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from keyInfo import cwhEmail, zzhEmail, zmEmail, senderEmail, senderEmailPassword 



# -----------------------------------------------------------------------------
class Email(object):
    def __init__(self):
        self.subjectPrefix = 'FCI'
        self.instrument = str() 
        self.receivers = [cwhEmail, zzhEmail]
    
    # -------------------------------------------------------------------------    
    def set_subjectPrefix(self, x):
        self.subjectPrefix = x

    def set_instrument(self, x):
        self.instrument = x

    def set_receivers(self, x):
        self.receivers = x
        
    # -------------------------------------------------------------------------
    def send(self, subject, content, png=None, mailBox=None):
        # 第三方 SMTP 服务
        mail_host = "smtp.exmail.qq.com"  # 设置服务器
        mail_user = senderEmail  # 用户名
        mail_pass = senderEmailPassword  # 口令

        sender = senderEmail
   
        # message = MIMEText(content, 'plain', 'utf-8')
        message= MIMEMultipart('related')
        message['From'] = Header("FCI", 'utf-8')
        message['To'] = Header("fcier", 'utf-8')

        subject = self.subjectPrefix + ' ' + self.instrument + ' ' + subject
        message['Subject'] = Header(subject, 'utf-8')

        msgAlter = MIMEMultipart('alternative')
        message.attach(msgAlter)
        msgAlter.attach(MIMEText(content, 'html', 'utf-8'))
        if png:
            for i in range(len(png)):
                cont_add = """
                <p><img src="cid:image""" + str(i) +""""></P>
                """
                msgAlter.attach(MIMEText(cont_add, 'html', 'utf-8'))
                fp = open(png[i], 'rb')
                msgImage  = MIMEImage(fp.read())
                fp.close()
                msgImage.add_header('Content-ID', '<image' + str(i) + '>')
                message.attach(msgImage)

        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
            smtpObj.login(mail_user, mail_pass)
            if mailBox is None:
                smtpObj.sendmail(sender, self.receivers, message.as_string())
            else:
                smtpObj.sendmail(sender, mailBox, message.as_string())
            print("mailing done.")
        except smtplib.SMTPException:
            print("Error: mailing failed!")


if __name__ == '__main__':
    email = Email()
    email.set_subjectPrefix('testing')
#    email.receivers = [cwhEmail]
    order={'order1': {'openPrice': 10, 'closePrice': 1, 'pnl': 9}}
    email.send('Horward', str(order), mailBox=[cwhEmail])
    
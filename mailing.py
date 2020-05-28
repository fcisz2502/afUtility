#!/usr/bin/python
# -*- coding: UTF-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from afUtility.keyInfo import cwhEmail, zzhEmail, zmEmail, senderEmail, senderEmailPassword, machineID


# -----------------------------------------------------------------------------
class Email(object):
    def __init__(self):
        self.machineID = machineID
        self.subjectPrefix = 'prefix'
        self.receivers = [cwhEmail, zzhEmail]
    
    # -------------------------------------------------------------------------    
    def set_subjectPrefix(self, x):
        self.subjectPrefix = x

    def set_receivers(self, x):
        self.receivers = x
        
    # -------------------------------------------------------------------------
    def send(self, subject, content, png=None, files=None, mailBox=None):
        # 第三方 SMTP 服务
        mail_host = "smtp.exmail.qq.com"  # 设置服务器
        mail_user = senderEmail  # 用户名
        mail_pass = senderEmailPassword  # 口令

        sender = senderEmail
   
        # message = MIMEText(content, 'plain', 'utf-8')
        message= MIMEMultipart('related')
        message['From'] = Header("FCI", 'utf-8')
        message['To'] = Header("fcier", 'utf-8')

        subject = self.machineID + "-" + self.subjectPrefix + ' ' + subject
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

        if files:
            for i in range(len(files)):
                file = files[i]
                filename = file.split("\\")[-1]
                att1 = MIMEText(open(file, 'rb').read(), 'base64', 'utf-8')
                att1["Content-Type"] = 'application/octet-stream'
                # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
                att1["Content-Disposition"] = 'attachment; filename="' + filename + '"'
                message.attach(att1)
        
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
#    email.set_subjectPrefix('testing')
#    email.receivers = [cwhEmail]
#    email.send('testing', str(), files=["C:\\applepy\\test.csv"])
#    order={'order1': {'openPrice': 10, 'closePrice': 1, 'pnl': 9}}
#    email.send('Horward', str(order), mailBox=[cwhEmail])
    
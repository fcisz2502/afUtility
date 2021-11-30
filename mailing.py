#!/usr/bin/python
# -*- coding: UTF-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from afUtility.keyInfo import (
    cwhEmail,
    zzhEmail,
    zmEmail,
    senderEmail,
    senderEmailPassword,
    mail_host,
    machineID
)


# -----------------------------------------------------------------------------
class Email(object):
    def __init__(self):
        self.machineID = machineID
        self.subjectPrefix = 'prefix'
        self.receivers = [cwhEmail]

    # -------------------------------------------------------------------------
    def set_subjectPrefix(self, x):
        self.subjectPrefix = x

    def set_receivers(self, x):
        self.receivers = x

    # -------------------------------------------------------------------------
    def send(self, subject, content, png=[], files=[], cc=[]):
        # 第三方 SMTP 服务
        _mail_host = mail_host  # 设置服务器
        mail_user = senderEmail  # 用户名
        mail_pass = senderEmailPassword  # 口令
        sender = senderEmail

        message = MIMEMultipart('related')
        message['From'] = Header("FCI", 'utf-8')
        message['To'] = Header("fcier", 'utf-8')

        subject = self.machineID + "-" + self.subjectPrefix + ' ' + subject
        message['Subject'] = Header(subject, 'utf-8')

        msgAlter = MIMEMultipart('alternative')
        message.attach(msgAlter)

        # add content to email
        mail_msg = """<p>""" + content + """</p>"""

        # attach id to each image
        for j in range(len(png)):
            mail_msg += """<p><img src="cid:image%s"></p>\n""" % j
        msgAlter.attach(MIMEText(mail_msg, 'html', 'utf-8'))

        # add images to email, images will not displayed if the nex three lines are deleted
        if png:
            for i, p in enumerate(png):
                with open(p, 'rb') as f:
                    msgImage = MIMEImage(f.read())
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
            smtpObj.connect(_mail_host, 25)  # 25 为 SMTP 端口号
            smtpObj.login(mail_user, mail_pass)

            if isinstance(cc, list):
                smtpObj.sendmail(sender, self.receivers + cc, message.as_string())
            else:
                # add alert information, then send email to receivers but cc
                # go to next line, use“\r\n” if is string, or "<br>" if html
                msgAlter.attach(
                    MIMEText(
                        "<br><br>cc Error: please input cc email addr in list, such as: ['xxx@xxx']",
                        'html',
                        'utf-8'
                    )
                )
                smtpObj.sendmail(sender, self.receivers, message.as_string())

            print("mailing done.")

        except smtplib.SMTPException:
            print("Error: mailing failed!")


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    email = Email()
    # email.set_subjectPrefix('testing')
    email.send('test', 'testing', png=["c:\\cwh\\probot_margin.png"])
    # order = {'order1': {'openPrice': 10, 'closePrice': 1, 'pnl': 9}}
    # email.send('Horward', str(order))


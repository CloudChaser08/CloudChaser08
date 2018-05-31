#
# Operators for interacting with SES
#
import boto3
import base64

from mimetypes import guess_type
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase

def send_email(source, destination, subject, body, files=None):
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = source
    msg['To'] = ', '.join(destination)

    msg_body = MIMEBase('text', 'plain')
    msg_body.set_payload(body)

    msg.attach(msg_body)
    
    for fn in files or []:
        mime = guess_type(fn)[0]
        with open(fn) as fin:
            file_content = fin.read()
        
        file_content = base64.b64encode(file_content)
        attachment = MIMEBase(mime.split('/')[0], mime.split('/')[1])
        attachment.add_header('Content-Disposition', 'attachment; filename="{}"'.format(fn.split('/')[-1]))
        attachment.add_header('Content-Transfer-Encoding', 'base64')
        attachment.set_payload(file_content)
        msg.attach(attachment)
    
    client = boto3.client('ses')
    client.send_raw_email(RawMessage={'Data' : msg.as_string()})

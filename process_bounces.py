#!/usr/bin/env python

import cymysql
import email
import imaplib
import re
import yaml

from pyquery import PyQuery as pq

SUBJECT_REGEX = (r"^ZOONIVERSE2?: (Daily error monitoring report|.* left the "
                 "list)$")

with open('/email.yml') as email_yml:
    email_credentials = yaml.load(email_yml)

M = imaplib.IMAP4_SSL(email_credentials['host'])

M.login(email_credentials['user'], email_credentials['pass'])

M.select()

reject_emails = set()
warnings = set()

typ, data = M.search(None, '(FROM "JISCMAIL")')

for num in data[0].split():
    typ, data = M.fetch(num, '(RFC822)')
    message = email.message_from_string(data[0][1])


    if re.match(SUBJECT_REGEX, message['subject']):
        print 'Processing "%s" from %s' % (message['Subject'], message['Date'])
        for part in message.walk():
            if(part.get_content_type()=="text/html"):
                body = str(part)
        parsed_html = pq(body)
        mailtos = parsed_html('a[href^=mailto]')

        try:
            split_index = body.index("currently being monitored")
        except:
            try:
                split_index = body.index("Original mail header")
            except:
                split_index = len(body)

        for email_link in mailtos:
            recip = email_link.get("href").split(':')[-1]
            try:
                email_index = body.index(str(recip))
            except:
                reject_emails.add(recip)

            if email_index < split_index:
                reject_emails.add(recip)
        M.store(num, '+FLAGS', '\\Deleted')
    else:
        print 'Ignoring "%s" from %s' % (message['Subject'], message['Date'])

M.close()
M.logout()

if len(reject_emails) == 0:
    exit()

print "Removed emails (%d total):" % len(reject_emails)

for e in reject_emails:
    print "* %s" % e

with open('/database.yml') as db_yaml:
    db_credentials = yaml.load(db_yaml)

prod = db_credentials['production']

conn = cymysql.connect(host=prod['host'], user=prod['username'],
                       passwd=prod['password'], db=prod['database'])
cur = conn.cursor()

cur.execute("UPDATE users SET valid_email = 0 WHERE email IN (%s) LIMIT %d" %
            (",".join([ conn.escape(e) for e in reject_emails]),
             len(reject_emails))
           )

print "Updated %d matching rows." % cur.rowcount

cur.close()
conn.close()

#!/usr/bin/env python

import datetime
import email
import imaplib
import psycopg2
import re
import smtplib
import sys
import time
import yaml

from email.mime.text import MIMEText

CHANGELOG_TIMELIMIT = datetime.timedelta(mins=30)

def generate_changelog_name():
    d = datetime.datetime.now()
    offset = d.day % 7
    if offset == 0:
        offset = 7
    d = d - datetime.timedelta(days=offset)
    week_letter = ['A', 'B', 'C', 'D', 'E'][int((d.day-1)/7)]
    return "CHANGELOG-%s%s" % (d.strftime("%Y%m"), week_letter)

with open('/email.yml') as email_yml:
    email_credentials = yaml.load(email_yml)

latest_changelog = generate_changelog_name()

listserv_address = 'listserv@jiscmail.ac.uk'
msg = MIMEText("GET ZOONIVERSE.%s" % latest_changelog)
msg['from'] = email_credentials['user']
msg['to'] = listserv_address

print "Requesting %s" % latest_changelog

s = smtplib.SMTP_SSL(email_credentials['smtp_host'])
s.login(email_credentials['user'], email_credentials['pass'])
s.sendmail(email_credentials['user'], [listserv_address], msg.as_string())
s.quit()

expected_subject = "File: ZOONIVERSE %s" % latest_changelog

print "Waiting for changelog to arrive",

changelog_cutoff = datetime.datetime.now() + CHANGELOG_TIMELIMIT

while True:
    if datetime.datetime.now() > changelog_cutoff:
        print "\nChangelog didn't arrive in time. Giving up."
        sys.exit(1)

    try:
        M = imaplib.IMAP4_SSL(email_credentials['host'])
        M.login(email_credentials['user'], email_credentials['pass'])
        M.select()
        typ, data = M.search(None, '(FROM "%s")' % listserv_address,
                             '(HEADER Subject "%s")' % expected_subject)
        if len(data) == 0 or len(data[0]) == 0:
            print ".",
            sys.stdout.flush()
            time.sleep(60)
            continue

        num = data[0]
        typ, data = M.fetch(num, '(RFC822)')
        message = email.message_from_string(data[0][1])

        changelog = message.as_string()
        M.store(num, '+FLAGS', '\\Deleted')
        break
    finally:
        M.close()
        M.logout()
        print ""

changes = {}

for line in changelog.split('\n'):
    m = re.match(
        r'(?P<timestamp>\d{14}) (?P<action>\w+) (?P<email>[^\s]+).*',
        line
    )
    if not m:
        continue
    timestamp, action, email = m.groups()
    changes.setdefault(action, set()).add(email)

removed_addresses = (
    list(changes.get('AUTODEL', [])) + list(changes.get('SIGNOFF', []))
)
removed_addresses = map(lambda s: s.lower(), removed_addresses)

print "Unsubscribing: "

for e in removed_addresses:
    print "* %s" % e

with open('/database.yml') as db_yaml:
    db_credentials = yaml.load(db_yaml)

prod = db_credentials['production']

conn = psycopg2.connect(
    host=prod['host'], user=prod['username'], password=prod['password'],
    dbname=prod['database']
)

try:
    cur = conn.cursor()
    cur.execute(
        ("UPDATE users SET global_email_communication = FALSE "
         "WHERE LOWER(email) = ANY(%s)"),
        (removed_addresses,)
    )
    conn.commit()
    print "Updated %d matching rows." % cur.rowcount
finally:
    cur.close()
    conn.close()

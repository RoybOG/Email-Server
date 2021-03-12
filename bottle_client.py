import requests
import sys
import re
import html
Server = "http://10.0.0.15:6061"


def send_email(email_dict):
    req = None
    try:
        req = requests.post(Server + "/send", data=email_dict)
        req.raise_for_status()

    except requests.exceptions.RequestException:
        print(html.unescape(re.search("<pre>(.+)</pre>", req.text).group(1)))
        sys.exit(1)


def get_emails(parameters_query_text):
    req = None
    try:
        req = requests.get(url=Server + "/getMail?" + parameters_query_text)
        req.raise_for_status()
        return req.json()
    except requests.exceptions.RequestException:
        print(html.unescape(re.search("<pre>(.+)</pre>", req.text).group(1)))
        sys.exit(1)


# send_email({"sender": "b", "recipient": "u", "title": "Jhon", "content": "rfe", "date": "10-4-2005"})

# print(get_emails("username=c&containtext=fe"))

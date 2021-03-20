import server_assistent_file as af
from wsgiserver import Server
from bottle import route, abort, request, post, default_app
import re
import datetime
from json import dumps
PORT = 6061

# CR: Empty function?
def check_content(title):

    return title

# CR: There is actually functions to check if date string is in the right format. It is better to use it rather than write your own
def check_date(user_date):
    max_date_size = 10
    # 2(days number) + 1(line) + 2(days number) + 1(line) + 4(years number) = 10
    date_format = '"<days(0-31)>-<month(1-12)>-<year>"'
    if len(user_date) > max_date_size:
        abort(400, '!{{! The date is not in the valid format. It should be: ' + date_format + "}}")

    scan = \
        re.search(r"^(?P<d>\d?\d)-(?P<m>\d?\d)-(?P<y>\d{4})", user_date)
    # re.search(r"^(?P<d>[0-9]|([1,2][0-9])|(3[0,1]))-(?P<m>[1-9]|(1[0-2]))-(?P<y>[1,2][9,0][0-9][0-9])", user_date)
    if not scan:
        abort(400, 'The date is not in the valid format. It should be: ' + date_format)
    r = scan.groupdict()
    try:
        user_date = datetime.date(int(r["y"]), int(r["m"]), int(r["d"]))
        if user_date > datetime.date.today():
            abort(401, "Your date is in the future, you can't enter future dates")

        if user_date < af.limit_date:
            abort(401, "Your date doesn't make sense, it is before {}, before there were even emails"
                  .format(af.limit_date))
        return user_date
        # Check also for impossible dates like 30th october. Search how to check if a date is on a calander.
    except ValueError as err:
        print(err.args[0])
        sep = err.args[0].find(": ")
        abort(400, err.args[0][0 if sep == -1 else sep:])


EMAIL_POST_PARAMETER_CHECK_FUNCTIONS = {
    af.EMAIL_POST_PARAMETER_NAMES["email_date"]: check_date
}


def get_user_post(field_names):
    """
    This function makes sure the post route using this will get all the fields it needs.
    If the user didn't post a certain field, it will raise an error and send a "bad request" error.
    :param field_names: a list all the name of the fields the route must get from the user
    :return: a dictionary of the names of the headers and the values.
    """
    user_values = {}
    for field_name in field_names:
        try:
            field_value = request.forms.get(field_name)
            if field_value:
                if field_name in EMAIL_POST_PARAMETER_CHECK_FUNCTIONS:
                    user_values[field_name] = EMAIL_POST_PARAMETER_CHECK_FUNCTIONS[field_name](field_value)
                else:
                    user_values[field_name] = field_value
            else:
                abort(400,
                      "Sorry, you haven't filled in your post request all this fields: {}, " +
                      "You missed the field {} to send an email.".format(str(field_names).strip("[]"), field_name))
        except ConnectionError:
            abort(420, "Sorry, something went wrong. Maybe it's due to your internet connections, try again later.")
    return user_values


@post('/send')
def send():
    user_details = get_user_post(list(af.EMAIL_POST_PARAMETER_NAMES.values()))

    user_email = af.Email(**user_details)
    af.save_email(user_email)


@route('/getMail')
def get_mail():
    args_to_search = dict(request.query)

    if args_to_search:
        if set(args_to_search.keys()).issubset(set(af.EMAIL_QUERY_NAMES.values())):

            if len(af.EMAIL_QUERY_NAMES["text"]) > af.TEXT_LIMIT:
                abort(404, "This program does not support to search a text this big. " +
                      "it needs to be less then or equal to 50 characters")

            if (af.EMAIL_QUERY_NAMES["from_date"] in args_to_search) ^ (
                    af.EMAIL_QUERY_NAMES["to_date"] in args_to_search):
                abort(404, "You cannot search for emails between dates if you haven't gave two dates")

            # As a part of the protocol, every parameter must have one key value. for example, when
            # The parameter is all the emails whose dates in a range between two dates, I save both of them
            # in the key "date_range"
            if af.EMAIL_QUERY_NAMES["from_date"] in args_to_search:
                if af.EmailSaveMode(af.configurations["save_emails_mode"]["value"]).name == "memory":
                    args_to_search["date_range"] = \
                            (af.get_date_num(check_date(args_to_search[af.EMAIL_QUERY_NAMES["from_date"]])),
                             af.get_date_num(check_date(args_to_search[af.EMAIL_QUERY_NAMES["to_date"]])))
                elif af.EmailSaveMode(af.configurations["save_emails_mode"]["value"]).name == "database":
                    args_to_search["date_range"] = {}
                    args_to_search["date_range"][af.EMAIL_QUERY_NAMES["from_date"]] = \
                        af.get_date_num(check_date(args_to_search[af.EMAIL_QUERY_NAMES["from_date"]]))
                    args_to_search["date_range"][af.EMAIL_QUERY_NAMES["to_date"]] = \
                        af.get_date_num(check_date(args_to_search[af.EMAIL_QUERY_NAMES["to_date"]]))

                args_to_search.pop(af.EMAIL_QUERY_NAMES["from_date"])
                args_to_search.pop(af.EMAIL_QUERY_NAMES["to_date"])

            return dumps(af.search_emails(args_to_search))

    abort(400, "You neither gave any parameters for the type of emails you want or didn't " +
          'fill the quarry correctly. \n The url should be exactly of the form: ' +
          '"/getMail?username={email address of the user who received all the emails}' +
          '&fromdt={day/month/year}&todt={day/month/year}&containstext={text}"' +
          "\nYou don't need to have all the parameters but they need to be only in this form.")


def main():
    import socket
    hostname = socket.gethostname()
    ip_addr = socket.gethostbyname(hostname)
    print("Server of IP {} is ready to go!".format(ip_addr))

    print("http://" + ip_addr + ':' + str(PORT))
    wsgiapp = default_app()
    httpd = Server(wsgiapp, port=PORT)
    httpd.serve_forever()
    # run(host='localhost', port=PORT)


try:
    main()
except KeyboardInterrupt:
    print("goodbye")
except Exception as e:
    print(e)
finally:
    af.finish()

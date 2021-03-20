import json
from ISA import Database, DatabaseThreads
import datetime
from functools import wraps
import threading
import os
import subprocess
import sys
from shutil import move, rmtree
import enum
import re
from atexit import register

from memory_resources import ResourceTree, ResourceDict, TreeNode

import bottle


def _excute_command(args, __times_crashed=0):
    """
    :param args:
    :param __times_crashed:
    :return:
    """

    p = subprocess.Popen(args, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=True, text=True)
    shell_output, error = p.communicate()
    shell_output = shell_output.strip()
    if p.returncode == 0:
        if shell_output:
            return shell_output.split('\n'), error
    else:
        print("Current Error: " + error)
        if __times_crashed < 3:
            __times_crashed += 1
            _excute_command(args, __times_crashed)

    return None, error


EMAIL_POST_PARAMETER_NAMES = {
    "from_user": "sender",
    "to_user": "recipient",
    "email_title": "title",
    "email_content": "content",
    "email_date": "date"
}

_EMAIL_POST_PARAMETER_DB_TYPES = {
    "to_user": "VARCHAR",
    "email_title": "TEXT",
    "email_content": "TEXT",
    "date_num": "INTEGER"
}

EMAIL_QUERY_NAMES = {
    "to_user": "username",
    "from_date": "fromdt",
    "to_date": "todt",
    "text": "containtext"
}

DB_QUERY_PARAMETERS = {
    EMAIL_QUERY_NAMES["to_user"]: "{}=:{}".format(EMAIL_POST_PARAMETER_NAMES["to_user"], EMAIL_QUERY_NAMES["to_user"]),
    "date_range": "date_num>=:{} and date_num<=:{}".format(EMAIL_QUERY_NAMES["from_date"],
                                                           EMAIL_QUERY_NAMES["to_date"]),
    EMAIL_QUERY_NAMES["text"]: "(instr({}, :{}) or instr({}, :{}))".format(EMAIL_POST_PARAMETER_NAMES["email_title"],
                                                                           EMAIL_QUERY_NAMES["text"],
                                                                           EMAIL_POST_PARAMETER_NAMES["email_content"],
                                                                           EMAIL_QUERY_NAMES["text"])
}

# Note, use "re.compile", for the _QUERy_TEMPLATE, it will compile the pattern once, and you can reuse it without
# compiling it over and over again when you put it on multiple chunks.
# query_pattern =

TEXT_LIMIT = 50
# The generic algorithm is that specific details of an email that perhaps in the future will would like to sort by
# (all the emails with "from_user" x or sent on a specific date), will be saved in a list above the title and the
# content
_QUERY_TEMPLATE = """<%header_text="{}: {}".format(headers[0], header_dict[headers[0]])%>
<%for header in headers[1:]: header_text+= ", {}: {}".format(header, header_dict[header]) end%>
{{header_text}}
{{email_title}}
{{email_content}}"""

__CONFIGURATION_TEMPLATE_SCAN = """^Configuration File For The Email Server


%for k in configurations.keys(): 

{{!r"{}= """ + r"\s*(?P<{}>\S+)\s*" + """{}".format(k, k, escape("(" + configurations[k].get("info","")+ ")"))}} 

%end
"""

__CONFIGURATION_TEMPLATE = """Configuration File For The Email Server


%for k in configurations.keys(): 

{{"{}= {}  ({})".format(k, configurations[k]["value"], configurations[k].get("info",""))}} 

%end
"""

_EMAIL_DETAILS = [EMAIL_POST_PARAMETER_NAMES["from_user"], EMAIL_POST_PARAMETER_NAMES["to_user"],
                  EMAIL_POST_PARAMETER_NAMES["email_date"]]

limit_date = datetime.date(1970, 1, 1)


# This is the year when emails(electronic mail) were invented and the first email "" was sent
# There can't be an email that was sent before that date.


def get_date_num(dt):
    return (dt - limit_date).days


# This is the date I finished programing and "launched" my service, the date people started to use this service.


def server_func_lock(func):
    """
    In contrast to preventing two threads from interaction with the database at the same time, This lock prevents from
    two threads doing a function at the same time, like two work server taking the same task to work on.
    This lock makes sure that the right functions will not be ran by two thread at once, which can disrupt -
 ail   :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        with server_lock:
            return func(*args, **kwargs)

    return wrapper


class EmailSaveMode(enum.Enum):
    database = 0
    memory = 1


class DataToFiles(enum.Enum):
    func_to_file = 0
    AddID = 1


def __find_desktop():
    comp_disks = re.findall("[A-Z]:", _excute_command(["fsutil", "fsinfo", "drives"])[0][0][7:])
    for d in comp_disks:
        home_dir = os.path.join(d, os.environ["HOMEPATH"], "Desktop")
        if os.path.isdir(home_dir):
            return home_dir

    raise WindowsError("Couldn't find desktop")


_FILE_NAMES = {"configuration_file": "email_server_configuration_file.txt",
               "DateNumTree": "DateNumTreeCache",
               "Ids": "IdsDbm",
               "recipients": "RecipientsDbm"}

_FOLDER_NAMES = {
    "root": "email_server",
    "memory": "memory",
    "database": "database"
}
FILE_DIRS = {"cache": "cache.txt"}
_BASE_DIR = _FOLDER_NAMES["root"]

desktop_dir = __find_desktop()
cache_vars = {"current_base_dir": "",
              "configuration_file_dir": os.path.join(desktop_dir, _FILE_NAMES["configuration_file"]),
              "current_email_id": 0, "num_of_date_calcs": 0}


def check_save_mode(user_mode):
    """
    checking the save mode the user entereed
    :param user_mode:
    """
    try:
        user_mode = int(user_mode)
    except (ValueError, SyntaxError):
        raise ValueError("You didn't enter a number for the email saving mode")
    try:
        _ = EmailSaveMode(user_mode)
    except ValueError:
        raise ValueError('There is no email mode for this number, '
                         'look for the description to the right of the value in parenthesise for the numbers of the '
                         'available modes.')

    return user_mode


def check_path(user_path):
    if not os.path.isdir(user_path.replace("\\", "/")):
        raise ValueError('The path you entered is not valid')

    return user_path.replace("/", "\\")


configurations = {
    "save_emails_mode": {"value": EmailSaveMode.memory.value,
                         "info": "{} = the emails will be saved in a sql database,".format(EmailSaveMode.database.value)
                                 + " {} = the emails will be saved on memory in files".format(
                             EmailSaveMode.memory.value),
                         "convert_function": check_save_mode},
    "base_dir": {"value": desktop_dir,
                 "info": "the full directory where the server will create the base folder in which"
                         " will have all the folders the program will need",
                 "convert_function": check_path}
}

__CONFIGURATION_TEMPLATE_PATTERN = \
    re.compile(bottle.template(__CONFIGURATION_TEMPLATE_SCAN, configurations=configurations, escape=re.escape),
               re.MULTILINE)

"""
A configuration has three data pieces:
The defualt value, the inforation(optional) that can be pressented beside the value and the convertion function,
that can check function(optional) whether the input is valid and convert it from text string to any type 
which will be returned.
"""


class Email:
    @server_func_lock
    def __init__(self, **kwargs):
        """
            :param from_user: The email address of the sender
            :param to_user: The email address of the recipient
            :param email_title: The title of the email
            :param email_content: the content of the email
        """
        global _QUERY_TEMPLATE
        self.__email_id = get_email_id()
        # I compare them as sets because sets are not ordered, so if they have the same keys but in a different order,
        # they will still be the same.
        if set(EMAIL_POST_PARAMETER_NAMES.values()) != set(kwargs.keys()):
            raise KeyError("The parameters of the object don't align with the parameters of an email")
        self.email_info = kwargs
        self.email_info["date_num"] = get_date_num(self.email_info[EMAIL_POST_PARAMETER_NAMES["email_date"]])
        self.__output_form = bottle.template(_QUERY_TEMPLATE, headers=_EMAIL_DETAILS, header_dict=self.email_info,
                                             email_title=self.email_info[EMAIL_POST_PARAMETER_NAMES["email_title"]],
                                             email_content=self.email_info[EMAIL_POST_PARAMETER_NAMES["email_content"]])

    @property
    def email_id(self):
        return self.__email_id

    def __str__(self):
        """
        Returns all the
        :return:
        """
        return self.__output_form

        # This method turns dates to number which I can compare them to get all the dates between the two given dates.
        # It won't make since for the number of days to be the number of days from 1 AD, the numbers will become too big
        # no one will save dates before anyone can use this service, so I'm counting since then.

    def get_info_for_db(self):
        info_to_db = {}
        for k in _EMAIL_POST_PARAMETER_DB_TYPES.keys():
            info_to_db[EMAIL_POST_PARAMETER_NAMES.get(k, k)] = self.email_info[EMAIL_POST_PARAMETER_NAMES.get(k, k)]
        info_to_db["full_email"] = str(self)
        return info_to_db

    # Since There is no cache it will create one.


def update_cache():
    with open("cache.txt", "w") as cache_f:
        json.dump(cache_vars, cache_f)


def update_from_cache():
    global cache_vars
    with open(FILE_DIRS["cache"]) as f:
        cache_vars = json.load(f)


"""
The generic algorithm for writing in files is that each text file writes the output of a function of the Email object.
If I will need to use another text file for another data, I will just have to write a function in the Email object
and add to this list
"""


def _search_for_file(file_name):
    """
    Searches a file in the  directory it is supposed to be in
    :param file_name: The full path of the file, all the way from the drive name to the name and file type(".txt",
    ".png"). It will scan the folder the file is supposed to be in according to the path, the last name of a folder
    in the path before the file name.
    :return: Return
    None if it failed to search or didn't find
    """
    # This takes on average 20 seconds
    # not_found_error = 'File Not Found\n'
    shell_output, search_error = _excute_command(["dir", file_name, "/s", "/b"])
    if shell_output and not search_error:
        return shell_output

    # Wheather it did not find the file or didn't succid to run due to an error, in both cases it is better for the
    # program to recreate the file in a place known to the program

    return None


def create_configuration_file():
    with open(cache_vars["configuration_file_dir"], 'w') as con_f:
        content = bottle.template(__CONFIGURATION_TEMPLATE, configurations=configurations)
        con_f.write(content)
        print("Created the file")


def create_files():
    """
    This will create all the files the program will read in
    """
    os.mkdir(_BASE_DIR)
    os.mkdir(os.path.join(_BASE_DIR, _FOLDER_NAMES["memory"]))
    # This will create empty text files

    os.mkdir(os.path.join(_BASE_DIR, _FOLDER_NAMES["database"]))


def setup_database():
    # when An ISA "Database" object is created, if the database doe not exist,
    # it creates a folder with the db file in it. This needs to happen here
    # This lock is to ensure that two clients don't interact with the same resource.
    if not emails_database.does_table_exists("Emails"):
        table_code = """
        CREATE TABLE "Emails" (
        "Id"	INTEGER,"""
        for par_name, par_type in _EMAIL_POST_PARAMETER_DB_TYPES.items():
            table_code += '\n\t"{}"  {} NOT NULL,'.format(EMAIL_POST_PARAMETER_NAMES.get(par_name, par_name), par_type)

        table_code += """
        "full_email"	TEXT NOT NULL, 
        PRIMARY KEY("Id" AUTOINCREMENT));
        """
        emails_database.create_table(table_code)


def read_configuration_file():
    global configurations
    try:
        with open(cache_vars["configuration_file_dir"], 'r') as textfile:
            scanned_file = __CONFIGURATION_TEMPLATE_PATTERN.search(textfile.read())

            if not scanned_file:
                raise SyntaxError(
                    "Your configuration file has been corrupted: The program couldn't read the configurations\n,"
                    " please delete the file, rerun the server and wait for it to recreate the file, fill the "
                    "configurations and then rerun again.")
            new_configurations = scanned_file.groupdict()

            if not new_configurations:
                raise SyntaxError(
                    "Your configuration file has been corrupted: The program couldn't read the configurations\n,"
                    " please delete the file, rerun the server and wait for it to recreate the file, fill the "
                    "configurations and then rerun again.")

            for con_key, user_value in new_configurations.items():
                configurations[con_key]["value"] = configurations[con_key].get("convert_function", lambda x: x)(
                    user_value)
    except OSError:
        raise OSError("Your configuration file has been corrupted: The program couldn't read the configurations\n," +
                      " please delete the file, rerun the server and wait for it to recreate the file, fill the " +
                      "configurations and then restart again.")

    # -Add a dictionary of check functions for every parameter. if there is none, skip, leave the checking
    # optional, just store the type in dictionary to convert from the file, e by default it will be a string
    # seperate from the con template to the re template


# ---SETUP---


if os.path.isfile(FILE_DIRS["cache"]):
    update_from_cache()
else:
    update_cache()
created_file = False
if not os.path.isfile(cache_vars["configuration_file_dir"]):
    file_dir = _search_for_file(cache_vars["configuration_file_dir"])
    if not file_dir:
        create_configuration_file()
        created_file = True
    elif len(file_dir) == 1:
        cache_vars["configuration_file_dir"] = file_dir[0]
    elif len(file_dir) > 1:
        print("The system detected more then one configuration file, please delete one of them for the system to work")
        sys.exit()

if not created_file:
    read_configuration_file()

_BASE_DIR = os.path.join(configurations["base_dir"]["value"], _FOLDER_NAMES["root"])

emails_database = None

if os.path.isdir(os.path.join(cache_vars["current_base_dir"], _FOLDER_NAMES["root"])):
    if cache_vars["current_base_dir"] != configurations["base_dir"]["value"]:
        move(os.path.join(cache_vars["current_base_dir"], _FOLDER_NAMES["root"]), _BASE_DIR)
else:
    if os.path.isdir(_BASE_DIR):
        rmtree(_BASE_DIR)
    create_files()

emails_database = Database("email_db", os.path.join(_BASE_DIR, _FOLDER_NAMES["database"]))
database_lock_handler = DatabaseThreads('email_db', os.path.join(_BASE_DIR, _FOLDER_NAMES["database"]))
setup_database()

# update the new configurations after I compare between the old and the new
cache_vars["current_base_dir"] = configurations["base_dir"]["value"]

update_cache()

server_lock = threading.RLock()


# ---ENDOFSETUP---
# CR: Nice!
_MEMORY_RESOURCES = {
    "DateNumTree": ResourceTree(os.path.join(_BASE_DIR, _FOLDER_NAMES["memory"], _FILE_NAMES["DateNumTree"]),
                                lambda email_obj: TreeNode(email_obj.email_info["date_num"],
                                                           email_id=email_obj.email_id), "date_range"),
    "Ids": ResourceDict(os.path.join(_BASE_DIR, _FOLDER_NAMES["memory"], _FILE_NAMES["Ids"]),
                        lambda email_obj: (email_obj.email_id, str(email_obj))),
    "recipients": ResourceDict(os.path.join(_BASE_DIR, _FOLDER_NAMES["memory"], _FILE_NAMES["recipients"]),
                               lambda email_obj:
                               (email_obj.email_info[EMAIL_POST_PARAMETER_NAMES["to_user"]],
                                email_obj.email_id), EMAIL_QUERY_NAMES["to_user"])}


@server_func_lock
def get_email_id():
    global cache_vars
    if EmailSaveMode(configurations["save_emails_mode"]["value"]).name == "memory":
        email_id = cache_vars["current_email_id"]
        cache_vars["current_email_id"] = cache_vars["current_email_id"] + 1
        return email_id

# CR: Exceptions in another file
class UnwantedEmail(Exception):
    def __init__(self):
        pass


@server_func_lock
def save_email_memory(user_email):
    for mem_resource in _MEMORY_RESOURCES.values():
        mem_resource.insert(user_email)


@database_lock_handler.execute_db_thread
def save_email_database(email, database_handler):
    database_handler.dump_data("Emails", email.get_info_for_db())


def search_email_memory(search_parameters):
    resources_results = []
    for mem_resource in _MEMORY_RESOURCES.values():
        if mem_resource.email_parameter_name in search_parameters:
            resources_results.append(set(mem_resource.scan(search_parameters)))

    wanted_details_emails = resources_results[0] if len(resources_results) == 1 else \
        resources_results[0].intersection(*resources_results[1:])

    wanted_emails = []

    for email_id in wanted_details_emails:
        try:
            email_text = _MEMORY_RESOURCES["Ids"].get_value(email_id)[0]
            if EMAIL_QUERY_NAMES["text"] in search_parameters:
                email_check = email_text[email_text.find('\n') + 1:]
                # as a part of the email format, the rule is that
                # the details are always in the first line and all the text(title and content) are in the next lines.
                if search_parameters[EMAIL_QUERY_NAMES["text"]] not in email_check:
                    raise UnwantedEmail
                """
                for more parameters about the text, just add them here! and if an email doesn't match these parameters,
                raise the custom exception "UnwantedEmail"
                ---more checks here

                """
            wanted_emails.append(email_text)
        except UnwantedEmail:
            pass

    return wanted_emails


@database_lock_handler.execute_db_thread
def search_email_database(search_parameters, database_handler):
    p_list = search_parameters.keys() & DB_QUERY_PARAMETERS.keys()

    par_text = []
    p_values = {}
    for par_name in p_list:
        if isinstance(search_parameters[par_name], dict):
            p_values.update(search_parameters[par_name])
        else:
            p_values[par_name] = search_parameters[par_name]

        par_text.append(DB_QUERY_PARAMETERS[par_name])

    return database_handler.collect_sql_quarry_result("select full_email from Emails where " + " and ".join(par_text)
                                                      + ";", quarry_args=p_values, filer_unique=True, rows_dict=False,
                                                      decode_rows=False)


# def save_email_database(from_user, to_user, email_title, email_content):


_EMAIL_SAVING_FUNCTIONS = {
    "database": save_email_database,
    "memory": save_email_memory
}
_EMAILS_SEARCHING_FUNCTIONS = {
    "database": search_email_database,
    "memory": search_email_memory
}


def save_email(user_email):
    _EMAIL_SAVING_FUNCTIONS[EmailSaveMode(configurations["save_emails_mode"]["value"]).name](user_email)


def search_emails(search_parameters):
    return _EMAILS_SEARCHING_FUNCTIONS[EmailSaveMode(configurations["save_emails_mode"]["value"]).name](
        search_parameters)


@register
def finish():
    """
    When the program closes, it updates the JSON variables.
    :return:
    """
    update_cache()
import sqlite3
import pickle
import os
import enum
from functools import wraps
import threading


class ColumnData(enum.Enum):
    cid = 0
    name = 1
    type = 2
    notnull = 3
    dflt_value = 4
    pk = 5


def handle_path(path):
    valid_path = '/'.join(list(filter(lambda x: len(x) > 0, path.replace('\\', '/').split("/"))))
    # print(valid_path)
    # print(os.path.isfile(valid_path))
    return valid_path


def remove_space(str_input):
    return str_input.replace(" ", "").replace("\n", "")


def find_with_different_cases(text, find_arg):
    return (text.lower()).index(find_arg.lower())


def create_folder(folder_dir):
    folder_path = handle_path(folder_dir)
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)


main_directory = handle_path(os.getcwd())
database_dir = main_directory + '/database'


# db_con = sqlite3.connect(database_dir + '/Main_Server_database.db')
# db_cursor = db_con.cursor()


class WritingError(Exception):
    """
    When the user enters values that contradict
    """
    pass


"""

    "type code" = the code that will be written in the querry to determine the type of the column
    limit size = 1
    dumping_function = 2
    loading_function = 3
"""

# Get rid of it!
data_types_encode = {
    int: {'type code': ['int', 'integer']},
    str: {'type code': ['varchar']},
    float: {'type code': ['float']},
    bytes: {'type code': ['blob']},
    type(None): {'type code': ['null']}
}


class DatabaseThreads:
    """
        I had a problem where a locked function called another locked function in the database module
        , but because the outer function already acquired the lock, so the inner function
        waited forever for it to be released.
        So I tried solving that by creating the flag "acquired_a_lock_on_this_thread" that tells the thread whether
        it was the one that acquired the lock. But found out I didn't need to reinvent the wheel and "RLock" exists
        spicifcally for this problem.
    """
    db_lock = threading.RLock()

    def __init__(self, database_name, db_loc=os.getcwd()):
        self._db_name = database_name
        self.db_loc = db_loc

    def execute_db_thread(self, func):
        """
        This decorator will create an sql Database object for a specific thread to use, and dispose of when the thread
        fininshed working with the database.
        The protocol of this method is that the last argument of the original function will be called "database_handler"
        ,which will be a Database object that the function will use to interact with the database, and the decorator
        will be the one who will fill this argument, so you don't need to.
        For example, the function "find_student(name, id, database_handler, just_check=False)" has "database_handler"
        as the last non-keyword argument. If you run find_student("jhonny", "123456789", just_check=True), This
        decorator will autimaticly supply the function with a Database object.
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            if args:
                if not type(args[-1]) == Database:
                    database_thread_handler = Database(self._db_name, db_loc=self.db_loc,
                                                       db_lock=DatabaseThreads.db_lock)
                    db_output = func(*args, **kwargs, database_handler=database_thread_handler)
                    del database_thread_handler
                else:
                    db_output = func(*args, **kwargs)
                return db_output

            if kwargs:
                if "database_handler" not in kwargs.keys():
                    database_thread_handler = Database(self._db_name, db_loc=self.db_loc,
                                                       db_lock=DatabaseThreads.db_lock)
                    db_output = func(*args, **kwargs, database_handler=database_thread_handler)
                    del database_thread_handler
                else:
                    db_output = func(*args, **kwargs)
                return db_output

        return wrapper


def thread_lock(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        if not isinstance(self, Database):
            self = kwargs["self"]
        if self._db_lock:
            with self._db_lock:
                output_value = func(*args, **kwargs)
            return output_value
        else:
            return func(*args, **kwargs)
    return wrapper


class Database:
    """
    This class handles a database, reads from it information and
    """
    __add_args_name = "add_args"
    __encoded_data_type = "longblob"

    # This module will automaicly decode anything in columns of data type "longblob" by unpickling.
    # This is how the module saves data types python supports but SQL doesn't like lists and dictionaries:
    # It pickles them.
    # SO the user MUST saves dictionaries and lists and such under the data type "longblob".

    def __init__(self, db_name, db_loc=os.getcwd(), db_lock=None):
        """

        :param db_name:
        :param db_loc:
        :param db_lock: This module supports threading. Because there is the limitation that two threads can't
        interact with a Sqlite server at the same time and sql objects can be used from one thread,
        so in project where multiple threads are communicating with the same database, locking is necessary.
        You can enter the lock object that all your threads use and all the interactions that this object will have with
        the database will be locked (if you don't require locks, then enter None and nothing will be different).
        Warning!!: The db_lock must be a Rlock to prevent from this module to get stuck.

        """

        self.db_name = db_name
        self._db_lock = db_lock
        self.row_factories = []
        if db_loc == 'memory':
            self.db_con = sqlite3.connect(':memory:')
            self.db_loc = None
        else:
            self.db_loc = handle_path(db_loc)
            self.db_folder = self.db_loc + '/database'
            create_folder(self.db_folder)
            self.db_file_dir = self.db_folder + '/' + self.db_name
            if not self.db_file_dir.endswith('.db'):
                self.db_file_dir += '.db'
            self.db_con = sqlite3.connect(self.db_file_dir)

        self.db_cursor = self.db_con.cursor()

    @thread_lock
    def execute_sql_code(self, sql_code, code_args=None):
        """
            Executes a code that changes the database, and saves the changes.
            It is important to know that when the program "dumps" data into a database, it encrypts data types SQL
            doesn't support by pickling them and saves them as bytes.
            :param sql_code: a string of a sql code.
            :param code_args: a dictionary/list of values you want to safely escape values into the code.

            If it's a dictionary, you escape values, by writing a certain string(a name), with a ":" before it
            in the code where you want to insert the value. Then in the dictionary, pair the value to that string
            (without the ":").
            If it's a list, then write "?" in the code where you want to insert the value.

            If you have in your code "?" and ":" and you want to escape both a list and a dictionary,
            then add to the dictionary a key "add_args"(for "additional arguments"), that is paired with the list.

            If you have threads in your project that use this module to interact with the database, this function will
            also make sure that every time the module executes sql code, it not happen at the same time.

        """
        with self.db_con:
            try:
                if code_args:
                    self.db_cursor.execute(sql_code, code_args)
                else:
                    self.db_cursor.execute(sql_code)

                self.db_con.commit()
            except sqlite3.Error as e:
                print('The sql code "{}" caused an error'.format(sql_code))
                raise e

    @thread_lock
    def collect_sql_quarry_result(self, sql_code, quarry_args=None, num_of_rows=None, filer_unique=True,
                                  rows_dict=True, decode_rows=True):
        """
        Asks for information from the database. It is returned in the form of a query.

        If you have threads in your project that use this module to interact with the database, this function will
        also make sure that every time the module collects a quarry, it not happen at the same time.
        :param sql_code: a string of a sql code
        :param num_of_rows: The number of top rows you want from a quarry result
        (if the argument is null then  it will just return all the rows it found)
        :param quarry_args:a dictionary/list of values you want to safely escape values into the code.
        :param filer_unique: if many rows are returned, but they're all one column,
        it will return a list of column values instead of a list of tuples with one value in them.
        :param rows_dict: will automatically configure a row to a dictionary of columns and the values
        in them. It will do this as default, if you want the quarry result as it is, enter false.
        :param decode_rows: Will decode complicated types you saved like dictioanries and lists, from bytes
        to their types.
        :return: a query as a list of rows. If it didn't find an rows, it will return None
        """

        prev_row_factory = self.db_cursor.row_factory
        l_func = self.__type_load
        if rows_dict:
            if decode_rows:
                self.db_cursor.row_factory = \
                    lambda r_cur, row: {k: l_func(sqlite3.Row(r_cur, row)[k]) for k in sqlite3.Row(r_cur, row).keys()}
            else:
                self.db_cursor.row_factory = sqlite3.Row
        else:
            if decode_rows:
                if filer_unique:
                    self.db_cursor.row_factory = lambda cursor, row: l_func(row[0]) if len(row) == 1 else \
                        [l_func(c) for c in row]

                else:
                    self.db_cursor.row_factory = lambda cursor, row: [l_func(c) for c in row]
            else:
                if filer_unique:
                    self.db_cursor.row_factory = lambda cursor, row: row[0] if len(row) == 1 else row

        with self.db_con:
            if quarry_args is None:
                quarry_args = []
            try:
                self.db_cursor.execute(sql_code, quarry_args)

                if not num_of_rows:
                    query_result = self.db_cursor.fetchall()
                elif num_of_rows > 0:
                    query_result = self.db_cursor.fetchmany(num_of_rows)
                else:
                    raise WritingError("The number of rows needs to be positive")
                self.db_cursor.row_factory = prev_row_factory
                return query_result
            except sqlite3.Error as e:
                print('The sql code "{}" caused an error'.format(sql_code))
                raise e

    def table_info(self, table_name):
        """
        Returns data on all the columns of a table.
        :param table_name: The name of the table.
        :return:
        "table_info" will return a tuple of a a dictionary of all the table's columns where each column name is a key
        to a list with information on that column
        and a list of the names of all the columns that hold encoded python types that SQL doesn't support.
        As a part of this module's protocol, They will always be of data type "longblob".
        It will return None if there are no tables.


        """
        raw_table_info = self.collect_sql_quarry_result("pragma table_info({});".format(table_name), rows_dict=False,
                                                        decode_rows=False)
        # This holds the information about the table as it is given by the Sqlite3 module: as a list of tuples.
        # For conveniency, I'll transform this into a dictionary, where each column_name is associated with its details.
        encrypted_columns = []
        # This will store all the names of the columns that are meant to store data types that SQL doesn't support.
        # Thier data type is "longblob" so the program can know to encrypt and decrypt them.
        if raw_table_info is None:
            return None, None

        info_dict = {}
        for info_column in raw_table_info:
            info_dict[info_column[ColumnData.name.value]] = info_column
            if info_column[ColumnData.type.value] == Database.__encoded_data_type:
                encrypted_columns.append(info_column[ColumnData.name.value])

        return info_dict, encrypted_columns

    def does_table_exists(self, table_name):
        """
        checks for the table exists in the user's database.
        I don't check if the table is in the main sql table, so hackers won't be able to gain access to the main table.
        :param table_name: the name of the table
        :return: True if it exists, False otherwise.
        """
        table_info = self.collect_sql_quarry_result("pragma table_info({});".format(table_name), rows_dict=False,
                                                    decode_rows=False)
        return bool(table_info)

    def create_table(self, table_sql_code, replace_table=False):
        """
        This function will check if a table with the same name already exists
        and if not, it will fill all the sizes of the column types according to the standard size for each type that
        is saved in the module.
        You just need to make sure that there is a space(and not a line break) from both sizes.
        There is a requirement that there needs to be a space(not break lines) around the column types
        and if a column is an auto increment, then it's type has to be "integer" so the program can
        distinguish it.
        :param table_sql_code: The "create" command without the limit sizes
        :param replace_table: If it's True and the table you want to create already exists, it will erase
        the current one and replace it with the
        """
        table_index = find_with_different_cases(table_sql_code, "TABLE")
        if table_index == -1:
            raise WritingError("This isn't a proper code for creating a table")
        end_index = find_with_different_cases(table_sql_code, "(")
        table_name = table_sql_code[table_index + 6:end_index - 1].strip()

        if self.does_table_exists(table_name):
            if replace_table:
                self.delete_table(table_name)
                self.execute_sql_code(table_sql_code)
        else:
            self.execute_sql_code(table_sql_code)

    @staticmethod
    def __type_load(encoded_value):
        """

        :param encoded_value: This value was loaded directly from the database
        :return:
        """
        try:
            return pickle.loads(encoded_value)
        except (pickle.PickleError, TypeError):
            return encoded_value

    @staticmethod
    def _transfer_list_to_dictionary(sql_code, dict_args, list_args):
        """
        This is transforming values that are ascaped via a list, to values that are escaped via a dictionary.
        :param sql_code:The original sql_code
        :param dict_args:A dictionary of arguments you want to add the list arguments.
        :param list_args:The list of arguments you want to escape.
        :return:
        """
        counter = 0
        s_point = 0

        f = sql_code.find("?", s_point + 1)
        while f > -1:

            if counter == len(list_args):
                raise WritingError("The amount of question marks don't fit the number of arguments")
            sql_code = sql_code[0:f] + ":a" + str(counter) + sql_code[f + 1:-1] + sql_code[-1]
            dict_args["a" + str(counter)] = list_args[counter]
            counter += 1
            f = sql_code.find("?", s_point + 1)

        return dict_args, sql_code

    def load_data(self, table_name, condition=None, select_args=None, select_columns=None, distinct=False, row_num=None,
                  order_by_columns=None, order_type="ASC", filer_unique=True, rows_dict=True):
        """
        Loads data from a table in the database by pulling certain rows with the "SELECT" sql command.

        :param table_name:the name of the table

        :param select_columns: A list of names of the columns you want load from. If it's "None", then it will load
        all the columns.

        :param distinct: "True" if you want to load rows that their values in the wanted columns are distinct,
        "False", if you want to load them normally.

        :param condition: the condition the rows need to satisfy to be loaded. The condition needs to be written in sql.
        Make sure the user can't affect the condition, because the user can do an injection.
        If condition is None, then it will return all the rows in the table.

        :param row_num: The top number of rows you want to be loaded from what it found.
        If it is "None", it will return all the rows it found.
        :param select_args: a dictionary/list of values you want to safely escape values into the code.

        :param order_by_columns: A list of column names where, if it's not None, the order of the rows of the querry
        will be sorted by the values of those columns
        :param order_type: This will determine if the rows will be sorted in ascending or descending order,
        . If it's "asc", then it will order the row from the row with the smallest values in the columns mentioned
        in "order_by_columns" to the biggest. If it's "DESC", then it will be the opposite.
        If it's None, it will automaticly sort by asending order.

        :param filer_unique: if querry returns one row, it will automaticlly return the row
        instead of a list of rows with one element.
        If you don't want this to happen, enter False.

        :param rows_dict: will automatically configure a row to a dictionary of columns and the values
        in them. It will do this as default, if you want the quarry result as it is, enter false.
        :return: a list of the rows it found.
         and the data in every row is presented as a dictionary of the column name and the value
         in that column.
           It returns None if it found no columns
        in that column
        """
        # print(table_info)
        select_code = "Select "
        if distinct:
            select_code += "distinct "

        if select_columns is None:
            select_code += "* "
        else:
            select_code += ", ".join(select_columns)

        select_code += " \nfrom " + table_name
        if condition:
            select_code += "\n where " + condition

        if order_by_columns:
            select_code += " \n order by " + ", ".join(order_by_columns)

            if order_type.lower() in ["asc", "desc"]:
                select_code += " " + order_type
            else:
                raise WritingError("This module doesn't support this ordering method")

        select_code += ";"
        raw_data = self.collect_sql_quarry_result(select_code, quarry_args=select_args, num_of_rows=row_num,
                                                  filer_unique=filer_unique,
                                                  rows_dict=rows_dict, decode_rows=True)
        # raw data holds all quarry as it is given by the sqlite3 module:
        #  a list of rows, each are each a tuple of values in each column by order.
        #  each row now will be translated it to a dictionary where the key of each value is the column name
        #  it is saved in.

        if raw_data is None:
            return None

        return raw_data

    def check_for_record(self, table_name, condition, distinct=False, distinct_columns=None, select_columns=None,
                         check_args=None, row_num=None, decode_rows=False, return_data=False):
        """
        Checks for records that satisfy a certain condition.
        You can't escape a table name and the condition, so you need to ensure that the user can't enter the table name
        or condition.
        :param table_name: the table name.

        :param distinct: if "return_data" is "True",
        "True" if you want to load rows that their values in the wanted columns are distinct,
        "False", if you want to load them normally.

        if "return_data" is "False" and "distinct" is "True", it will check if all the values are
        distinct. It will return

        :param distinct_columns: if "distinct" is True, then it can get a list of columns the user would like to check
        has distinct values(If distinct is False, then it will ignore it)
        :param condition: the condition the rows need to satisfy to be loaded. The condition needs to be written in sql.
        Make sure the user can't affect the condition, because the user can do an injection.

        :param select_columns: A list of names of the columns you want load from if "return_data" is "True".
        If it's "None", then it will load all the columns.
        :param row_num: The top number of rows you want to be loaded from what it found.
        If it is "None", it will return all the rows it found.

        :param check_args: a dictionary/list of values you want to safely escape values into the code of the condition.

        :param return_data: "True" for loading all the rows
        or "False" for just checking if there are records that satisfy the condition.
        :param decode_rows: Will decode complicated types you saved like dictioanries and lists, from bytes
        to their types.
        :return: if "return_data" is "True", it will return a list of all the wanted rows
        (None if there are none) and if "return_data" is "False",
        it will return "True" if there are rows that satisfy the condition, "False" if there are None.
        If "distinct" is True and "Return_Data" is false, then it will also return if the condition is met and if what
        fulfilled the conditions is distinct as a tuple.
        """
        if not self.does_table_exists(table_name):
            raise WritingError("The table doesn't exist")

        if return_data:

            result = self.load_data(table_name, condition, select_args=check_args, distinct=False,
                                    select_columns=select_columns, row_num=row_num)
            return result
        else:
            sql_code = "Select * from {} where {};".format(table_name, condition)
            result = self.collect_sql_quarry_result(sql_code, quarry_args=check_args, filer_unique=False,
                                                    decode_rows=decode_rows)
            if distinct:
                if result is None:
                    return False, False
                info_dict, encrypted_columns = self.table_info(table_name)

                if distinct_columns is None:
                    distinct_columns = info_dict.keys()

                for column_name in distinct_columns:
                    if column_name not in info_dict:
                        raise WritingError("The column doesn't exist")
                    sql_code = "Select distinct {} from {} where {};".format(column_name, table_name, condition)
                    dis_result = self.collect_sql_quarry_result(sql_code, quarry_args=check_args,
                                                                filer_unique=False, decode_rows=decode_rows)
                    if len(dis_result) < len(result):
                        return True, False
                return True, True
            return result is not None

    def find_specific_record(self, table_name, values, distinct=False, select_columns=None,
                             row_num=None, return_data=False):
        """
        Returns records that have a specific values in a specific columns.
        :param table_name: the table name.

        :param values: a dictionary with the name of a column pared with the value you want to check in that column.

        :param distinct: if "return_data" is "True",
        "True" if you want to load rows that their values in the wanted columns are distinct,
        "False", if you want to load them normally.

        if "return_data" is "False" and "distinct" is "True", it will check if all the values are
        distinct. It will return

        :param select_columns: A list of names of the columns you want load from if "return_data" is "True".
        If it's "None", then it will load all the columns.

        :param row_num: The top number of rows you want to be loaded from what it found.
        If it is "None", it will return all the rows it found.

        :param return_data: "True" for loading all the rows
        or "False" for just checking if there are records with these values.

        :return: if "return_rows" is "True", it will return a list of all the wanted rows
        (None if there are none) and if "return_rows" is "False",
        it will return "True" if there are rows that have these values, "False" if there are None.
        """

        condition = ""
        info_dict, encrypted_columns = self.table_info(table_name)
        if info_dict is None:
            raise WritingError("The table doesn't exist")
        first_column = True
        check_values = {}
        for column_name, value in values.items():
            if column_name not in info_dict:
                raise WritingError("You wanted to check a column that doesn't exist")

            if first_column:
                first_column = False
            else:
                condition += " and "

            if value is None:
                condition += '{} is null'.format(column_name)
            else:
                condition += '{}=:column_{}'.format(column_name, column_name)
                check_values["column_" + column_name] = self.__type_dump(value,
                                                                         info_dict[column_name][ColumnData.type.value])

        return self.check_for_record(table_name, condition, distinct=distinct, distinct_columns=list(values.keys()),
                                     select_columns=select_columns, check_args=check_values, row_num=row_num,
                                     return_data=return_data)

    def delete_records(self, table_name, condition, con_args=None):
        """
        It deletes rows that satisfy the condition.
        :param table_name: the table name.
        :param con_args: a dictionary/list of values you want to safely escape values into the code of the condition.
        :param condition: the condition the rows need to satisfy to be deleted.
        The condition needs to be written in sql.
        Make sure the user can't affect the condition, because the user can do an injection.
        """

        delete_code = "delete from {} where {};".format(table_name, condition)
        self.execute_sql_code(delete_code, con_args)

    def update_records(self, table_name, values, condition=None, code_args=None):
        """
        Updates specific columns with new values in rows that satisfy the condition.
        :param table_name: the table name.

        :param values: a dictionary with the name of a column pared with the new value you update.

        :param condition: the condition the rows need to satisfy to be updated.
        If it's None, it will fill all the rows that has in a column a different value then the value you want to update
        in that column(Even though it is preferable to fill a condition).
        The condition needs to be written in sql.
        Make sure the user can't affect the condition, because the user can do an injection.

        :param code_args: A dictionary of arguments to escape into the condition. If you HAVE to escape a list, you can,
        but it will be transferred to a dictionary, which will be slow the program significantly.
        of code you want to safely escape into the code of the condition.
        If it's a dictionary, the keys need to be names that are present in where you want to escape the value to in
        the code, with ":" before it.
        """

        if not self.does_table_exists(table_name):
            raise WritingError("The table doesn't exist")

        update_code = "update " + table_name + "\n set "
        condition_code = "not ("
        info_dict, encrypted_columns = self.table_info(table_name)

        first_column = True
        value_esc = {}
        for column_name, value in values.items():
            if column_name not in info_dict:
                raise WritingError("You wanted to check a column that doesn't exist")
            if info_dict[column_name][ColumnData.pk.value] == 0:
                # The function makes sure it won't update primary keys, becuase it would be hard to find a row
                # when it'sidentifier was changed. Also it will turn a couple of primary keys to the same value.
                if first_column:
                    first_column = False
                else:
                    condition_code += " and "
                    update_code += ", "

                condition_code += '{}=:column_{}'.format(column_name, column_name)
                update_code += '{}=:column_{}'.format(column_name, column_name)
                value_esc["column_" + column_name] = Database.__type_dump(value,
                                                                          info_dict[column_name][ColumnData.type.value])

        condition_code += ")"
        if condition is None:
            update_code += "\n where " + condition_code + ";"
        else:
            update_code += "\n where " + condition + ";"

        if code_args is not None:
            if isinstance(code_args, dict):
                value_esc.update(code_args)
            elif isinstance(code_args, list):
                # This is the reason why it is prefered to ascape to an update command a dictioanry, becuase it can't
                # ascape the dictionary of values to update and the list of arguments at the same time.
                # So the program will inevitably
                value_esc, update_code = self._transfer_list_to_dictionary(update_code, value_esc, code_args)
            else:
                raise WritingError('The "code_args" is not a dictionary or a list.')

        self.execute_sql_code(update_code, code_args=value_esc)

    @staticmethod
    def __type_dump(column_value, column_type):
        """
        How this module handles data types that SQL doesn't support but python does, is by ecrpyting them via pickling,
        and saving them in a column marked by the data_type "longblob", to
        Handles a value that the program wants to write in the database by decoding it by it's special protocol.
        The protocol enables saving python types.
        :param column_value:the value you want to encrypt.
        :return: the
        """

        if type(column_value) in data_types_encode:
            return column_value
        else:
            if column_type == Database.__encoded_data_type:
                return pickle.dumps(column_value)
            else:
                raise WritingError("You are saving a data type that SQL doesn't support in a regular column.\n" +
                                   'The column needs to have the data type of "{}"'.format(Database.__encoded_data_type)
                                   + ' so that the module would know to encrypt and decrypt it.')

    def dump_data(self, table_name, insert_dict):
        """
        Inserts a row of data to a table.
        :param table_name: The name of the table you want to insert your row in.
        :param insert_dict: a dictionary filled with columns from the table
         and the data you want to enter in that column.
        """

        info_dict, encrypted_columns = self.table_info(table_name)
        if not info_dict:
            raise WritingError("The table doesn't exist")

        safe_dictionary = {}
        # This dictioanry is a copy of "insert_dict" and includes all the values you want to dump.
        # It will encrypt it by the module's protocol and then safely escape it into the sql code to prevent SQL
        # injection.

        for column in insert_dict:
            safe_dictionary[column] = Database.__type_dump(insert_dict[column],
                                                           info_dict[column][ColumnData.type.value])

        new_row_code = "INSERT INTO " + table_name + '(' + ', '.join(insert_dict.keys()) + ')'
        new_row_code_values = ' VALUES (:' + ', :'.join(insert_dict.keys()) + ')'

        # To prevent level one sql injection I'm won't direcly insert the values to the code,
        # but safely escape them into the code

        new_row_code += new_row_code_values + ';'
        self.execute_sql_code(new_row_code, safe_dictionary)

    def create_function(self, sql_function, sql_function_name):
        self.db_con.create_function(sql_function_name, sql_function.__code__.co_argcount, sql_function)

    def close(self):
        # print('Connection closed')
        self.db_cursor.close()
        self.db_con.close()

    def __del__(self, delete_database=False):
        # self.execute_sql_code("DETACH DATABASE " + self.db_name)
        self.close()
        """
        if delete_database:
            if os.path.isfile(self.db_file_dir):
                os.remove(self.db_file_dir)
        """

    def delete_table(self, table_name):
        if self.does_table_exists(table_name):
            self.execute_sql_code("DROP TABLE " + table_name)

    def reset_table(self, table_name):
        """
        It Erases all of a tables rows.
        :param table_name: The name of the table.
        """

        if self.does_table_exists(table_name):
            self.execute_sql_code("DELETE FROM " + table_name)

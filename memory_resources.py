from interface import implements, Interface
from pickle import dump, load, dumps, loads
import os
from atexit import register
import dbm


class Resource(Interface):
    def __init__(self, file_dir, email_input_func=lambda x: x, email_parameter_name=None):
        """
        You should call this AFTER the implemented __init__
        :param file_dir:

        """
        self.file_dir = file_dir
        self.email_input_func = email_input_func
        self.email_parameter_name = email_parameter_name

        register(self.backup)

    def load(self):
        pass

    def insert(self, email_obj):
        pass

    def scan(self, search_parameters):
        pass

    def backup(self):
        pass


class TreeNode:
    def __init__(self, num_value, *, left=None, right=None, email_id=None):
        self.__num_value = num_value
        self.left = left
        self.right = right
        if email_id is None:
            self.__email_ids = []
        else:
            self.__email_ids = [email_id]

    @property
    def email_ids(self):
        return self.__email_ids

    @property
    def num_value(self):
        return self.__num_value

    def add_id(self, new_email_id):
        if new_email_id not in self.__email_ids:
            self.__email_ids.append(new_email_id)

    def __str__(self):
        return "({}, {})".format(self.num_value, self.__email_ids)

    # CR: In contrary to the ISA file, this function does needs documatation. Most of the recursive functions does
    @staticmethod
    def traversal_in_order_of_the_dates(tree_node, min_num, max_num):
        if not tree_node:
            return []
        if tree_node.num_value < min_num:
            return TreeNode.traversal_in_order_of_the_dates(tree_node.right, min_num, max_num)

        if tree_node.num_value > max_num:
            return TreeNode.traversal_in_order_of_the_dates(tree_node.left, min_num, max_num)

        return TreeNode.traversal_in_order_of_the_dates(tree_node.left, min_num, max_num) + tree_node.email_ids + \
            TreeNode.traversal_in_order_of_the_dates(tree_node.right, min_num, max_num)


class ResourceTree(implements(Resource)):

    def __init__(self, file_dir, email_input_func=lambda x: x, email_parameter_name=None):
        Resource.__init__(self, file_dir, email_input_func, email_parameter_name)
        if os.path.isfile(file_dir):
            self.load()
        else:
            self.__tree = None

    def load(self):
        with open(self.file_dir, 'rb') as f:
            self.__tree = load(f)

    def insert(self, email_obj):
        new_element = self.email_input_func(email_obj)
        # with open(self.file_dir, 'ab') as f:
        #    dump((new_element.num_value, new_element.email_ids), f)

        if self.__tree:
            temp = self.__tree
            inserted = False
            while not inserted:
                if temp.num_value < new_element.num_value:
                    if temp.right:
                        temp = temp.right
                    else:
                        temp.right = new_element
                        inserted = True
                elif temp.num_value > new_element.num_value:
                    if temp.left:
                        temp = temp.left
                    else:
                        temp.left = new_element
                        inserted = True
                elif temp.num_value == new_element.num_value:
                    temp.add_id(new_element.email_ids[0])
                    inserted = True

        else:
            self.__tree = new_element

    def scan(self, search_parameters):
        min_num, max_num = search_parameters[self.email_parameter_name]
        if "date_range" not in search_parameters:
            raise TypeError
        return TreeNode.traversal_in_order_of_the_dates(self.__tree, min_num, max_num)

    def backup(self):
        print("Backed up")
        with open(self.file_dir, 'wb') as f:
            dump(self.__tree, f)


class ResourceDict(implements(Resource)):
    def __init__(self, file_dir, email_input_func=lambda x: x, email_parameter_name=None):
        self.dict_handler = None
        Resource.__init__(self, file_dir, email_input_func, email_parameter_name)

    def load(self):
        pass

    def insert(self, email_obj):
        new_key, new_value = self.email_input_func(email_obj)
        if isinstance(new_key, int):
            new_key = str(new_key)
        v = self.get_value(new_key)
        if new_value not in v:
            v.append(new_value)
        with dbm.open(self.file_dir, 'c') as dict_handler:
            dict_handler[new_key] = dumps(v)

    def get_value(self, k):
        if isinstance(k, int):
            k = str(k)
        with dbm.open(self.file_dir, 'c') as dict_handler:
            v = dict_handler.get(k)
            if v:
                return loads(v)
            return []

    def scan(self, search_parameters):
        k = search_parameters[self.email_parameter_name]
        return self.get_value(k)

    def backup(self):
        pass

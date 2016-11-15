import os
import re
import sys
import cx_Oracle
from tabulate import tabulate
import readline
import csv

CMD_QUIT = 'q'

CMD_SAVE_NEXT = 'sn'
CMD_SAVE_PREV = 'sp'

CMD_LIST_VIEWS = 'dv'
CMD_LIST_TABLES = 'dt'

CMD_SET_SAVE_FORMAT = 'sf'
CMD_SET_OUTPUT_FORMAT = 'of'
CMD_SET_EXTENDED = 'x'

# Output Data

VALID_OUTPUT_FORMATS = [
    "plain",
    "simple",
    "grid",
    "fancy_grid",
    "psql",
    "pipe",
    "orgtbl",
    "rst",
    "mediawiki",
    "html",
    "latex",
    "latex_booktabs"]


HISTORY_FILE = "{0}/.osql.history".format(os.environ['HOME'])
hist_fd = open(HISTORY_FILE, 'a')
hist_fd.close()


class Completer:

    def __init__(self, words):
        self.words = words
        self.prefix = None

    def complete(self, prefix, index):
        if prefix != self.prefix:
            # we have a new prefix!
            # find all words that start with this prefix
            self.matching_words = [
                word for word in self.words if word.startswith(prefix)
                ]
            self.prefix = prefix
        try:
            return self.matching_words[index]
        except IndexError:
            return None


service_name = ""
hostname = ""
port = ""

tnsname = """(DESCRIPTION =
  (ADDRESS_LIST =
    (ADDRESS =
      (PROTOCOL = TCP)
      (HOST = {0})
      (PORT = {1}))
  )
  (LOAD_BALANCE = yes) (FAILOVER = ON)
  (CONNECT_DATA =
    (SERVICE_NAME = {3})
    (FAILOVER_MODE =
      (TYPE = select)
      (METHOD = basic)
      (RETRIES = 20)
      (DELAY = 5)
    ) (UR = A)
  )
)""".format(hostname, port, service_name)


username = ""
password = ""

connection_str = "{user}/{passwd}@{tnsname}".format(user=username,
                                                    passwd=password,
                                                    tnsname=tnsname)

conn = cx_Oracle.connect(connection_str)
cursor = conn.cursor()

RE_COMMAND = re.compile(r"^\\(?P<command>.*)")

var_extended = False
var_save_format = 'csv'
var_output_format = 'psql'


def shutdown():
    readline.write_history_file(HISTORY_FILE)
    sys.stdout.write('\n')
    sys.exit(0)


def save_query_results(cursor, sql, path, fmt='csv'):

    try:
        with open(path, 'w') as fd:

            queries = list(x for x in sql.split(';') if bool(x.strip()))

            if len(queries) > 1:
                sys.stderr.write("Query Saver will only save the first query submitted.")

            sql = queries.pop()

            cursor.execute(sql)
            cols = list([desc[0] for desc in cursor.description])
            rows = cursor.fetchall()

            global var_save_format
            if var_save_format == 'csv':

                writer = csv.writer(fd, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_ALL)

                writer.writerow(cols)
                writer.writerows(rows)
            else:
                fd.write(tabulate(rows, cols, tablefmt=var_save_format))

    except OSError:
        print "?"



def describe(cursor, relation):
    relation = relation.replace(';', '').strip()
    cursor.execute('SELECT * FROM {0} WHERE 1=0'.format(relation))
    return cursor.description


def remove_last_history_item():
    last = readline.get_current_history_length() - 1
    readline.remove_history_item(last)


def run_command(command, cursor):

    args = command.strip().split()

    command = args.pop(0).lower()

    if command == CMD_QUIT:
        shutdown()
    elif command == CMD_SET_EXTENDED:
        global var_extended
        var_extended = bool(args[0].lower() == 'on')
    elif command == CMD_SAVE_NEXT:
        sys.stderr.write('NOT IMPLEMENTED\n')
    elif command == CMD_SET_SAVE_FORMAT:

        if len(args) and args[0].lower() in (VALID_OUTPUT_FORMATS + ['csv']):
            global var_save_format
            var_save_format = args[0].lower()
            sys.stdout.write("SAVE_FORMAT = {0}\n".format(var_save_format))

    elif command == CMD_SET_OUTPUT_FORMAT:

        if len(args) and args[0].lower() in (VALID_OUTPUT_FORMATS + ['csv']):
            global var_output_format
            var_output_format = args[0].lower()
            sys.stdout.write("OUTPUT_FORMAT = {0}\n".format(var_output_format))

    elif command == CMD_SAVE_PREV:

        path = '/tmp/osql_save'

        if len(args):
            path = ' '.join(args)

        last = readline.get_current_history_length() - 1
        sql = readline.get_history_item(last)

        print sql

        save_query_results(cursor, sql, path)

    elif command == CMD_LIST_TABLES:
        cursor.execute("SELECT TABLE_NAME, OWNER FROM ALL_TABLES")
        rows = cursor.fetchall()
        print tabulate(rows, ['Name', 'Owner'], tablefmt='psql')
    elif command == CMD_LIST_VIEWS:
        SQL = "SELECT VIEW_NAME, OWNER FROM ALL_VIEWS"
        if len(args):
            SQL += " WHERE VIEW_NAME LIKE '%{0}%'".format(args[0])
        cursor.execute(SQL)
        rows = cursor.fetchall()
        if len(rows) > 1:
            print tabulate(rows, ['Name', 'Owner'], tablefmt='psql')
        else:
            print tabulate(describe(cursor, args[0]),
                           ['name', 'type', 'display_size', 'internal_size',
                            'precision', 'scale', 'null_ok'], tablefmt='psql')
    else:
        sys.stderr.write("Unknown command.\n")

try:
    readline.read_history_file(HISTORY_FILE)
    cursor.execute("SELECT TABLE_NAME FROM ALL_TABLES UNION SELECT VIEW_NAME FROM ALL_VIEWS")

    words = set()
    for (name, ) in cursor:
        words.add(name)

    completer = Completer(words)

    readline.parse_and_bind("tab: complete")
    readline.set_completer(completer.complete)

    cursor.arraysize = 10000
    while True:

        data = raw_input("SQL> ")

        command = RE_COMMAND.match(data)

        if command:
            run_command(command.group('command'), cursor)

        # ENTER SQL MODE
        else:

            try:
                while ';' not in data:
                    remove_last_history_item()
                    data += " " + raw_input("  -> ")
            except KeyboardError:
                continue

            try:
                remove_last_history_item()
                queries = data.split(';')
                for query in queries:
                    if bool(query.strip()):
                        readline.add_history(query + ';')
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        if cursor.rowcount:
                            cols = list([desc[0] for desc in cursor.description])
                            if var_extended:
                                for row in rows:
                                    print tabulate(zip(cols, row), ['COLUMN', 'VALUE'], tablefmt='psql')
                            else:
                                print tabulate(rows, cols, tablefmt=var_output_format)
                        print("ROWS {0}".format(cursor.rowcount))
            except cx_Oracle.DatabaseError as error:
                print error
except EOFError:
    shutdown()
except KeyboardInterrupt:
    shutdown()


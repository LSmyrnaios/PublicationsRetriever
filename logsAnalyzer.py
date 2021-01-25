"""
This program analyzes the logfiles and extracts useful info :-)
"""

import sys
import os
import zipfile
import re
from datetime import datetime
import datetime

log_files_directory = None
file_no = 0
MAX_TIME_DIFFERENCE = 30  # 30 seconds
pre_date_time = None
errors_found = 0
specific_string_counter = 0
string_to_be_found = "The maximum limit ("


def get_time_object(line):
    try:
        date_time_string = re.search("(.*)\\s(?:INFO|DEBUG|WARN|ERROR).*", line).group(1)
    except:
        date_time_string = None

    if date_time_string is None or len(date_time_string) == 0:
        print("There was a problem when retrieving the time-string from the line:\n" + line, file=sys.stderr)
        return None

    try:
        date_time_string += "000"  # Add 3 zeros to simulate the "microseconds" at the end, since there's no support for milliseconds :-P
        # print("date_time_string: " + date_time_string)  # DEBUG!
        time_object = datetime.datetime.strptime(date_time_string, "%Y-%m-%d %H:%M:%S.%f")
    except:
        return None

    return time_object


def analyze_time_difference(current_line, previous_line, log_file):
    global pre_date_time
    current_date_time = get_time_object(current_line)
    if current_date_time is None:
        # print("No date_time_string for line: " + current_line) # DEBUG!
        return

    # Take the date and time of the current line
    if pre_date_time is None:
        pre_date_time = current_date_time
        return

    time_difference = abs(current_date_time - pre_date_time)
    # print("time_difference: " + str(time_difference))

    if time_difference > datetime.timedelta(seconds=MAX_TIME_DIFFERENCE):
        print("Large time difference (" + str(time_difference) + " > " + str(MAX_TIME_DIFFERENCE) + " seconds) for the following lines of logging_file: " + log_file)
        print(previous_line + current_line)

    pre_date_time = current_date_time


def check_contained_strings(current_line):
    global errors_found, specific_string_counter, string_to_be_found
    if "ERROR" in current_line:
        print("A line with an \"ERROR\" found:\n" + current_line)
        errors_found += 1
    elif string_to_be_found in current_line:
        # print("A line containing the string \"" + string_to_be_found + "\" found:\n" + current_line)   # DEBUG!
        specific_string_counter += 1


def load_and_check_log_files():
    # Open directory and show the files inside:
    print("Opening log_directory: " + log_files_directory)
    for file in os.scandir(log_files_directory):

        file_name = file.name
        full_file_path = os.path.join(log_files_directory, file_name)

        if file_name.endswith(".zip"):
            with zipfile.ZipFile(full_file_path, 'r') as zip_ref:
                zip_ref.extractall(".")
                file_list_size = len(zip_ref.filelist)
                if file_list_size != 1:
                    print("The zip-file \"" + file_name + "\" contained more than one files (" + str(file_list_size) + ")!")
                    continue
                file_name = zip_ref.filelist[0].filename
                print("File name inside zip: " + file_name)
                zip_ref.extractall(log_files_directory)  # Just extract in the logs directory.
            full_file_path = os.path.join(log_files_directory, file_name)

        if not file_name.endswith(".log"):
            print("Found a non \"LOG-file\": " + file_name)

        print("Opening log_file: " + str(file_name))
        previous_line = ""

        with open(full_file_path, 'r+') as log_file:
            for current_line in log_file:
                if len(current_line) == 0 or current_line == "\n":
                    continue

                # print("Current_line: " + current_line) # DEBUG

                analyze_time_difference(current_line, previous_line, log_file.name)

                check_contained_strings(current_line)

                # Other metrics here

                previous_line = current_line  # Before moving to the next line


if __name__ == "__main__":
    args = sys.argv
    num_args = len(args)
    if num_args != 2:   # The first arg is the program's name and the second is the "log_files_directory".
        print("Invalid number of argument given: " + str(num_args-1) + "\nExpected to get the \"log_files_directory\".", file=sys.stderr)
        exit(1)

    log_files_directory = args[1]
    if not os.path.isdir(log_files_directory):
        print("The following directory does not exists: " + log_files_directory + "\nRerun the program with a valid \"log_files_directory\"..", file=sys.stderr)
        exit(2)

    load_and_check_log_files()

    print("Number of \"ERROR\"-instances found: " + str(errors_found))
    print("Number of instances of \"" + string_to_be_found + "\" found: " + str(specific_string_counter))
    exit(0)

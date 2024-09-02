"""
Given the path os a directory, design a method to traverse a directory and list all the files, grouping them by the extensions.

'/C:/Users/thean/PycharmProjects/PyLeetProblems/data'
"""

import os
from collections import defaultdict

def traverse_and_group_by_extension(directory_path):
    # Dictionary to hold files grouped by their extension
    files_by_extension = defaultdict(list)

    # Debugging: Check if the directory exists
    if not os.path.exists(directory_path):
        print(f"Directory '{directory_path}' does not exist.")
        return files_by_extension

    # Walk through the directory
    for root, dirs, files in os.walk(directory_path):
        print(f"Currently in directory: {root}")  # Debugging
        for file in files:
            print(f"Found file: {file}")  # Debugging
            # Split the file name and extension
            _, extension = os.path.splitext(file)
            # Add the file to the list for its extension
            files_by_extension[extension].append(os.path.join(root, file))

    return files_by_extension


# Example usage:
directory_path = 'C:/Users/thean/PycharmProjects/PyLeetProblems/data'  # Make sure this path is correct
grouped_files = traverse_and_group_by_extension(directory_path)

# Print the grouped files
if grouped_files:
    for extension, files in grouped_files.items():
        print(f"Extension: {extension if extension else 'No extension'}")
        for file in files:
            print(f" - {file}")
else:
    print("No files found.")


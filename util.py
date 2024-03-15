import os

def create_ifnotexists_directory(directory_name):
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)
#!/usr/bin/env python
import sys
import os

os.system("cp classroom.yml .github/workflows/classroom.yml")

student_repo_path="../csc-369-student/"
if os.path.isdir(student_repo_path):
    print("Updating %s"%student_repo_path)
    cmd = "cd %s && git pull"%student_repo_path
    r = os.system(cmd)
    if r != 0:
        print("Command failed:",cmd)
        exit(1)
else:
    cmd = "cd .. && git clone https://github.com/anderson-github-classroom/csc-369-student.git"
    r = os.system(cmd)
    if r != 0:
        print("Command failed:",cmd)
        exit(1)

path = os.getcwd()

# correction in case this is another repo
#last = path.split("-")[-1]
#c = 0
#finished = False
#while not finished:
#    try:
#        int(last)
#        path = path[:(-len(last)-1)]
#    except:
#        finished = True

identifier = "-".join(path.split("/")[-1].split("-")[:2])
print("Identifier:",identifier)

subdir = None
if "lab-" in identifier:
    print("Auto-detected that this is a lab")
    subdir="labs"
elif "tutorial-" in identifier:
    print("Auto-detected that this is a tutorial")
    subdir="tutorials"
else:
    print("Auto-detected that this is an assignment")
    subdir="assignments"

if subdir != "assignments":
    name = "".join([c[0].upper()+c[1:] for c in identifier.split("-")])
else:
    name = identifier.split("-")[0]
    name = name[0].upper()+name[1:]
print("Name:",name)

if subdir is not None:
    if os.path.isfile("%s.ipynb"%name):
        print("File already exists. Rename it and run this program again if you want a fresh copy.")
        exit(1)

    print("Copying %s*"%name)
    cmd = "cp %s%s/%s* ."%(student_repo_path,subdir,name)
    r = os.system(cmd)
    if r != 0:
        print("Command failed:",cmd)
        exit(1)

    print('You now have your assignment/lab')
     

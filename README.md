# CSC 369 Template Lab
This is a blank repo containing information about working on and submitting your labs and assignments.

## Get the lab in the recommended way
python ./get_assignment.py # this should work assuming your directory name hasn't been munged

## If that doesn't work, you can get the lab manually
* Issue the following command once (not needed again) on your system:
    * ``git clone https://github.com/anderson-github-classroom/csc-448-student ../csc-448-student``
* Copy the lab/assignment notebook over

## Local autograding
* Issue this command everytime you want to test. It will try to automatically detect what assignment you are working on.
    *  ./run_tests_locally.sh or python run_tests_locally.py
* You can always try to test specific questions using:
    * pytest ../csc-369-student/tests/test_Syllabus.py::test_question_1

## Remove autograding
Use git to add/commit/push. Check under GitHub actions

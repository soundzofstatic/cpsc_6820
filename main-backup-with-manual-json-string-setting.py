# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import re
import json

# def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    # print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
# if __name__ == '__main__':
    # print_hi('PyCharm')

file1 = open('data/amazon-meta.txt', 'r')
Lines = file1.readlines()

jsonFile = open('amazon-meta.json', 'w')


structureStarted = False
structureEnded = False

jsonContent = ''
jsonContentList = []

jsonObjectString = ''
jsonObjectDictionary = {}

count = 0

# totalLines = len(Lines)
totalLines = 43

jsonFile.writelines('[\n')

# Strips the newline character
for line in Lines:
    count += 1

    if count < 3:
        continue

    if count > 43:
        break

    emptyLine = True if line.strip() == '' else False

    # JSON Object open
    if emptyLine and not(structureStarted):
        structureStarted = True
        jsonObjectString += '{'

    # print("Line{}: {}".format(count, line.strip()))
    if not(emptyLine):
        # jsonObjectDictionary["Name"].append("Guru")

        regexKeySearch = re.findall(r"(.*?)\:", line.lstrip())
        if len(regexKeySearch) > 0: # Key:value pair found
            print(regexKeySearch[0])
            regexValueSearch = re.findall(r"\: (.*)", line.lstrip())
            print(regexValueSearch)

            # jsonObjectDictionary[regexKeySearch[0]].append(regexValueSearch if len(regexValueSearch) > 0 else 'unknown')
            jsonObjectDictionary[regexKeySearch[0]] = regexValueSearch[0].strip() if len(regexValueSearch) > 0 else 'unknown'


        print("Leading Spaces: ", len(line) - len(line.lstrip()))


    # JSON Object close
    if emptyLine and structureStarted and not(structureEnded):
        structureEnded = True
        jsonObjectString += '}'

        if(count < totalLines):
            jsonObjectString += ',\n'
        else:
            jsonObjectString += '\n'

    # JSON Object write to file
    if structureStarted and structureEnded:
        # jsonFile.writelines(jsonObjectString)
        if (count < totalLines):
            jsonFile.writelines(json.dumps(jsonObjectDictionary) + ',\n')
            # jsonObjectString += ',\n'
        else:
            jsonFile.writelines(json.dumps(jsonObjectDictionary))
            # jsonObjectString += '\n'
        # jsonFile.writelines(json.dumps(jsonObjectDictionary))
        jsonContentList.append(jsonObjectDictionary)

        # Resets the Booleans in prep for the next object
        structureStarted = False
        structureEnded = False

        # Reset the string in order to keep RAM usage down
        jsonObjectString = ''
        jsonObjectDictionary = {}



print(jsonContentList)

jsonFile.writelines(']')

jsonFile.writelines(jsonContent)
jsonFile.close()
jsonFile.close()





# See PyCharm help at https://www.jetbrains.com/help/pycharm/

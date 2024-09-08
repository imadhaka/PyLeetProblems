"""
* Repeat the character of string upto the digit next
*
* input: adca3rtyy2uid3
* Expected output:
* aaadddcccaaa
* rrttyyyy
* uuuiiiddd

*********************************
************ ALGO ************

Take two variables and Iterate given list
If numeric found, break. Current = end & return numeric value
Set start as first & end as last of string & Get the character string of digit long

"""

import re

class RepeatCharacter:
    def repeat_characters(self, input_string):
        result = []
        i = 0

        # Loop through the input string
        while i < len(input_string):
            # Collect characters until we encounter a digit
            chars = []
            while i < len(input_string) and not input_string[i].isdigit():
                chars.append(input_string[i])
                i += 1

            # Get the digit if available and repeat the characters
            if i < len(input_string) and input_string[i].isdigit():
                repeat_count = int(input_string[i])
                repeated_chars = ''.join(chars) * repeat_count
                result.append(repeated_chars)
                i += 1

        return '\n'.join(result)

#inputstr = input("Enter a string: ")
inputstr = "adca3rtyy2uid3"
if not (inputstr[len(inputstr)-1].isdigit()):
    raise Exception("Please enter a string ending with 0-9.")
s1 = RepeatCharacter()
value = s1.repeat_characters(inputstr)
print(value)
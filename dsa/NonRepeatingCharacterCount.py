"""
find non-repeating character from a string and return the count with each character
"""

class NonRepeatingCharactercount:
    def count_non_repeating_characters(self, s):
        # Step 1: Create a dictionary to store character frequencies
        char_count = {}

        # Step 2: Count each character's frequency
        for char in s:
            char_count[char] = char_count.get(char, 0) + 1

        # Step 3: Filter and return the non-repeating characters and their counts
        non_repeating = {char: count for char, count in char_count.items() if count == 1}

        return non_repeating


ob = NonRepeatingCharactercount()
# Test the function
input_string = "abracadabra"
result = ob.count_non_repeating_characters(input_string)
print(result)
#Given a string s, find the length of the longest substring without repeating characters.

####### ALGO #######

# Create a Hash Set
# Declare two iterator variables, say i & j
# Iterate over string till i & j < len(str)
# if set contains current character, remove character at i from set & i++
# This will iterate till that current character is present
# Else add character to set & j++
# max length will be difference between two iterators & max

####### ALGO #######


def longestsubstr(str):
    myset = set([])
    mx = 0
    i = 0
    j = 0
    while i < len(str) and j < len(str):
        if myset.__contains__(str[j]):
            myset.remove(str[j])
            i += 1
        else:
            myset.add(str[j])
            j += 1
            mx = max(j-i,mx)
    return mx


inp = input("Enter any string: ")
print("Length of longest substring without repeating characters is: " + str(longestsubstr(inp)))

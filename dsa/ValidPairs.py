"""
# Write a function that takes in a non-empty array of distinct integers and an integer representing a target sum.
# If any two numbers in the input array sum up to the target sum, the function should return them in an array in any order.
# If no two numbers sum up to the target sum, the function should return an empty array.


# Sample input
array = [3,5,-1,8,11,1,6]
targetSum = 10
# Sample output [-1,11]
"""

class ValidPairs:
    def getValidPairs(self, array, targetSum):
        seen = {}
        pairs = []

        for num in array:
            value = targetSum - num
            if value in seen:
                pairs.append([value, num])
            seen[num] = True
        return pairs

ob = ValidPairs()
array = [3,4,-1,8,11,1,6]
targetSum = 10
print(ob.getValidPairs(array, targetSum))
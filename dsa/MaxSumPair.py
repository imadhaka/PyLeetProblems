'''
Given an array of elements, find a pair of numbers which has the maximum sum.
Sample Array: [ 3, 67, 6, 34, 54, 89, 76 ]

Approach:
-- Declare initial variable from array for initial sum
-- Iterate twice for the array
-- If sum of current iteration is greater the previous sum, update the values
'''

class MaxSum:
    def getSumPair(self, arr):
        a = arr[0]
        b = arr[1]
        for i in range(len(arr)):
            for j in range(i+1, len(arr)):
                if arr[i] + arr[j] > a+b:
                    a = arr[i]
                    b = arr[j]
        return a, b


arr = [ 3, 67, 6, 34, 54, 89, 76 ]
ob = MaxSum()
a, b = ob.getSumPair(arr)
print(a, b)
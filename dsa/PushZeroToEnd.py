#Push all the zeroes of array at the end without changing the sequence

"""
****** ALGO ******
Iterate over array
Copy non-zero elements to new array
Add zero at the end for rest elemets
"""

def pushZero(arr):
    cnt = 0
    i = 0
    while i < len(arr):
        if arr[i] != 0:
            arr[cnt] = arr[i]
            cnt += 1
        i += 1
    while cnt < len(arr):
        arr[cnt] = 0
        cnt += 1
    return arr

arr = [0, 1, 0, 3, 1, 4, 0, 6, 7]
zeroarr = pushZero(arr)
print(arr)
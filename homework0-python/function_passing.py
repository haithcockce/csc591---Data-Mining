#!/bin/python

import sys



def num_ones_in_binary(x=0):
    """Count the number of 1s of a number when converted to binary. 

    ARGUMENTS:
        x
            int to convert to binary (default: 0).
    RETURNS:
        Int representing the count of 1s in the binary version of arg `x`
    """
    return bin(x)[2:].count('1')


def most_ones_in_binary(nums):
    """Return the number with the most 1s when converted to binary.

    ARGUMENTS: 
        nums
            list of ints to search through. 
    RETURNS:
        int with the most 1s in the list of numbers
    """
    one_counts = map(num_ones_in_binary, nums)
    return nums[one_counts.index(max(one_counts))]


def num_digits(x):
    return len(str(x))

def most_digits(L):
    L = sorted(L, key=num_digits)
    return L[-1]

def largest_two_digit_even(L):
    two_digit_numbers = [i for i in L if num_digits(i)==2]
    evens = [i for i in two_digit_numbers if i%2==0]
    evens.sort()
    return evens[-1]

def best(L, criteria):
    return criteria(L)

L = [1, 76, 84, 95, 214, 1023, 511, 32]
print(best(L, min)) # Prints 1
print(best(L, largest_two_digit_even)) # Prints 84
print(best(L, most_digits)) # Prints 1023
print(most_ones_in_binary(L))


import random

def generate_17_digit_number():
    # Generate a random 17-digit number
    number = random.randint(10**16, 10**17 - 1)
    return number

def generate_9_digit_number():
    # Generate a random 9-digit number
    number = random.randint(10**8, 10**9 - 1)
    return number

# Test the methods
print("17-digit number:", generate_17_digit_number())
print("9-digit number:", generate_9_digit_number())

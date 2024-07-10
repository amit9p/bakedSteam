

import random

def generate_9_digit_number():
    return random.randint(100000000, 999999999)

# Example usage
random_number = generate_9_digit_number()
print("Generated 9-digit number:", random_number)

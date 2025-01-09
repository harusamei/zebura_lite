import hashlib

def string2id(input_string):
    # Create a SHA-256 hash object
    hash_object = hashlib.md5()
    
    # Encode the input string and update the hash object
    hash_object.update(input_string.encode('utf-8'))
    
    # Get the hexadecimal representation of the hash
    hex_dig = hash_object.hexdigest()
    
    return hex_dig

if __name__ == '__main__':
    # Example usage
    input_string = "example_string"
    unique_id = string2id(input_string)
    print(f"Unique ID for '{input_string}': {unique_id}")
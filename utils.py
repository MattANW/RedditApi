from time import time

def is_english(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True
    

def filter_characters(s: str) -> str:
    return ''.join(char for char in s if str.isalnum(char))

class TwoWayMap:
    def __init__(self, forward_type, backward_type) -> None:
        self.forward_type = forward_type
        self.backward_type = backward_type

        self.forward_map = {}
        self.backward_map = {}

    def get_forward(self, key):
        return self.forward_map.get(key, False)
    
    def get_backward(self, key):
        return self.backward_map.get(key, False)
    
    def get(self, key):
        if type(key) == self.forward_type:
            return self.get_forward(key)
        elif type(key) == self.backward_type:
            return self.get_forward(key)
        else:
            raise Exception(f"{key} is an invalid key type for this TwoWayMap. Should be either {self.forward_type} or {self.backward_type}!")
        
    def set(self, forward, backward):
        self.forward_map[forward] = backward
        self.backward_map[backward] = forward
        
def binary_search(tables, value):
    while len(tables) > 0:
        mid_index = len(tables) // 2
        mid_table = tables[mid_index]
        
        if mid_table == value:
            return True
        elif mid_table < value:
            tables = tables[mid_index + 1:]
        else: 
            tables = tables[:mid_index]
    return False


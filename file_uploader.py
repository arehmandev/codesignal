from collections import defaultdict

class FileManager:

    default_value = {"size": int(), "timestamp": int(), "ttl": None}
    files = None

    def __init__(self) -> None:
        self.files = defaultdict(lambda: self.default_value.copy())

    def FILE_UPLOAD(self, file_name, size):
        if file_name in self.files:
            raise Exception(f"File already exists: {file_name}")
        
        self.files[file_name]["size"] = size

    def FILE_COPY(self, source, dest):
        if source not in self.files:
            raise Exception(f"File doesn't exist: {source}")
        if dest in self.files:
            raise Exception(f"File already exists: {dest}")
                
        self.files[dest] = self.files[source].copy()

    def FILE_GET(self, file_name, return_full=False):
        if file_name in self.files:
            if return_full:
                return self.files[file_name]
            return self.files[file_name]["size"]
        return None

    def FILE_SEARCH(self, prefix, file_list_param=None, item_count=10):
        if not file_list_param:
            file_list_param = self.files

        file_list = [
            {**{"name":x}, **y} for (x,y) in file_list_param.items() 
                     if x.startswith(prefix)
                     ]
        file_list.sort(key=lambda x: x["name"],reverse=True)
        file_list.sort(key=lambda x: x["size"], reverse=True)

        return file_list[:item_count]
    
    def FILE_UPLOAD_AT(self, timestamp, file_name, file_size, ttl=None):
        self.FILE_UPLOAD(file_name, file_size)
        self.files[file_name]["timestamp"] = timestamp
        if ttl:
            self.files[file_name]["ttl"] = ttl

    def FILE_GET_AT(self, timestamp, file_name):
        f = self.FILE_GET(file_name, True)
        if not f:
            return None
        size = None
        if f["timestamp"] <= timestamp:
            size = f["size"]
            if f["ttl"]:
              if f["ttl"] + f["timestamp"] < timestamp:
                  size = None
        
        return size
    
    def FILE_COPY_AT(self, timestamp, file_from, file_to):
        if not self.FILE_GET_AT(timestamp, file_from):
            raise Exception(f"File doesn't exist: {file_from} at timestamp {timestamp}")
        if self.FILE_GET_AT(timestamp, file_to):
            raise Exception(f"File already exists: {file_to} at timestamp {timestamp}")

        self.FILE_COPY(file_from, file_to)
        self.files[file_to]["timestamp"] = timestamp
        

    def FILE_SEARCH_AT(self, timestamp, prefix, item_count=10):
        valid_timestamp_files = {x:y for (x,y) in self.files.items() 
                                 if not y["timestamp"] or y["timestamp"] <= timestamp
                                 }
        invalid_ttl_files = {x:y for (x,y) in valid_timestamp_files.items() 
                             if y["ttl"] and y["ttl"] + y["timestamp"] < timestamp
                             }
        final_files = {x:y for (x,y) in valid_timestamp_files.items() if x not in invalid_ttl_files}
        return self.FILE_SEARCH(prefix, final_files, item_count)
    

    def ROLLBACK(self, timestamp):
        files = self.FILE_SEARCH_AT(timestamp, "", len(self.files))
        self.files = { v['name']: {"size": v["size"], "timestamp": v['timestamp'], "ttl": v["ttl"]}  for v in files}

        

test = FileManager()
test.FILE_UPLOAD("file1.txt", 100)
try:
    test.FILE_UPLOAD("file1.txt", 100)
except Exception as e:
    assert str(e) == "File already exists: file1.txt"

test.FILE_COPY("file1.txt", "file2.txt")
assert test.files["file2.txt"]["size"] == 100

assert test.FILE_GET("file2.txt") == 100
assert test.FILE_GET("file3.txt") == None

for i in range(3,10):
    test.FILE_UPLOAD(f"file{i}.txt", i*100)


max_files = test.FILE_SEARCH("file")
assert(len(max_files)) == 9
assert(max_files[0]["size"] == 900)
assert(max_files[8]["name"] == "file1.txt")

test.FILE_UPLOAD_AT(1200, "file10.txt", 999)
assert(test.files["file10.txt"]["size"] == 999)
assert(test.files["file10.txt"]["timestamp"] == 1200)
assert(test.files["file10.txt"]["ttl"] == None)

try:
    test.FILE_UPLOAD_AT(1200, "file10.txt", 999)
except Exception as e:
    assert(str(e) == "File already exists: file10.txt")

assert test.FILE_GET_AT(1200, "file10.txt") == 999
assert test.FILE_GET_AT(1201, "file10.txt") == 999
assert test.FILE_GET_AT(1199, "file10.txt") == None

test.FILE_COPY_AT(2000, "file10.txt", "file11.txt")
assert test.FILE_GET_AT(2000, "file11.txt") == 999
assert test.files["file11.txt"]["timestamp"] == 2000


test.FILE_UPLOAD_AT(1200, "file_11_ttl.txt", 969, 50)
assert test.FILE_GET_AT(1199, "file_11_ttl.txt") == None
assert test.FILE_GET_AT(1200, "file_11_ttl.txt") == 969
assert test.FILE_GET_AT(1250, "file_11_ttl.txt") == 969
assert test.FILE_GET_AT(1251, "file_11_ttl.txt") == None

assert len(test.FILE_SEARCH_AT(100, "file", 20)) == 9
assert len(test.FILE_SEARCH_AT(1250, "file", 20)) == 11

assert len(test.files) == 12 

test.ROLLBACK(1250)
assert len(test.files) == 11

test.ROLLBACK(1251)
assert len(test.files) == 10

test.ROLLBACK(100)
assert len(test.files) == 9

print("Success")

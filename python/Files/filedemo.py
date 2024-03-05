# Assuming data.txt is in the same directory as this Python script

file_path = "data.txt"

try:
    with open(file_path, "r") as file: #f = open("data.txt", "r")
       # data = file.read()
        print(file.readline())
        print("check")
except FileNotFoundError:
    print("File not found. Please check the file path.")

finally:
    file.close()

try:
    f = open("data.txt", "r")
    f1 = open('abc.txt','w')
    for data in f:
        f1.write(data)
except Exception as e:
    print(e)


finally:
    
    f1.close()
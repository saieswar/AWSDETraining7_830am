f = open("data.txt", "r")
print(f.readline())
print(f.readline())

f1 = open("data.txt", "r")
f2 = open("abc.txt", 'w')
for data_of_file in f1:
    f2.write(data_of_file)

f3 = open("data.txt", "r")
f4 = open("abc.txt", 'w')
for data_of_file in f1:
    f2.write(data_of_file)
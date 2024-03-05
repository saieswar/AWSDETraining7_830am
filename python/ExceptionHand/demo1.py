#

# 1) first write proper synatax - a/b example with out error 
# 2) change b value to 0 and ask?  -- runtime error
# 3) add try and except block with custom msg and e 

# 4) Explain file open and close before and after a/b 
# 5) finally can be shown 

# 6) take input from key board inside try block and convert to int - give str as i/p 

# 7) Now show custom msg making sense?

# 8) NOw u can make step 6 outside try and check the type fo error 

# 9) then write 2 except blocks and explain 

# 10) General error with Exception class 








a = 5
b= 0

try:
    print(a/b)
except Exception as e:
    print("Hey you can't divide by 0")
    print(e)

print("bye")

x= 9
y = 8
try:
    print("File Opened")
    print(x/y)
    print("File Closed")
except Exception as e:

    print("Hey you can't divide by 0")
    print(e)





x= 9
y = 0
try:
    print("File Opened")
    print(x/y)
    
except Exception as e:

    print("Hey you can't divide by 0")
    print(e)

finally:
    print("File Closed")
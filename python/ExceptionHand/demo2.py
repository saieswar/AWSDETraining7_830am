a = 6
b = 3


try:
    print("open File") 
    print("Enter input")
    x = int(input())
    print(x)
    print(a/b)
except ZeroDivisionError as z:
    print("Handling Zero Division", z)
except ValueError as v:
    print("Handling Input Value Error", v)   
except Exception as e:
    print("Any Error i dont know", e)

finally:
    print("File close")



print("Bye karthi")

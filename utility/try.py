def is_divisible(x,y):
    if x % y ==0:
        return True
    else:
        return False

def is_power(num,num1):
    if num == num1:
        return True
    if num1 == 1 and num == 1:
        return True
        
    while is_divisible(num,num1):
        num = num/num1
    return num == 1


print(is_power(8,2))

# def get_dimnesions(l,w):
#     return l+1, w+5

# def main(length_,width):
#     l,w = get_dimnesions(length_, width)
#     print(l,w)

# l = int(input("Enter length"))
# w = int(input("Enter width"))

# main(l,w) 
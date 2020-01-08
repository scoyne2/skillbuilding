
def is_palindrome(num):
    # Skip single-digit inputs
    if num // 10 == 0:
        return False
    temp = num
    reversed_num = 0

    while temp != 0:
        reversed_num = (reversed_num * 10) + (temp % 10)
        temp = temp // 10

    if num == reversed_num:
        return True
    else:
        return False

def palindromes():
    num = 0
    while True:
        if is_palindrome(num):
            i = (yield num)
            if i is not None:
                num = i
        num += 1

pal_gen = palindromes()
for i in pal_gen:
    print(i)
    digits = len(str(i))
    if digits == 5:
        #pal_gen.throw(ValueError("We don't like large palindromes"))
         pal_gen.close()
    pal_gen.send(10 ** (digits))


nums_squared_gc = (num**2 for num in range(5))

print(next(nums_squared_gc))
print(next(nums_squared_gc))
print(next(nums_squared_gc))

####################################################

file_name = "techcrunch.csv"
lines = (line for line in open(file_name)) #generator that reads file
list_line = (s.rstrip()split(",") for s in lines) #generator that splits lines 
cols = next(list_line) #first line is the header

company_dicts = (dict(zip(cols, data)) for data in list_line) #generator that creates a dictionary
funding = (
    int(company_dict["raisedAmt"])
    for company_dict in company_dicts
    if company_dict["round"] == "A"
)#generator that gets raised amount if round is A

total_series_a = sum(funding)

print(f"Total series A fundraising: ${total_series_a}")

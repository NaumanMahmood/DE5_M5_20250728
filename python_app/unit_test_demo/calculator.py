class Calculator:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def get_sum(self):
        return self.a + self.b
    
    def get_diff(self):
        return self.a - self.b
    
    def get_product(self):
        return self.a * self.b
    
    def get_quotient(self):
        return self.a / self.b

    def get_sqrt(self):
        return self.a**2

if __name__ == "__main__":
    myCalc = Calculator(a=2, b=5)
    
    answer1 = myCalc.get_sum()
    answer2 = myCalc.get_diff()
    answer3 = myCalc.get_product()
    answer4 = myCalc.get_quotient()

    myCalc2 = Calculator(a=5)
    answer5 = myCalc2.get_sqrt()

    print(answer1)
    print(answer2)
    print(answer3)
    print(answer4)
    print(answer5)
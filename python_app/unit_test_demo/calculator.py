import pandas as pd

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

    def get_sqrt(x):
        return x**0.5
    
    def times_table(x, y=10):
        data = {'Result': [x * i for i in range(1, y+1)]}
        return pd.DataFrame(data)

if __name__ == "__main__":
    myCalc = Calculator(a=934, b=34)
    
    #answer1 = myCalc.get_sum()
    #answer2 = myCalc.get_diff()
    answer3 = myCalc.get_product()
    #answer4 = myCalc.get_quotient()
    answer5 = Calculator.get_sqrt(25)

    df = Calculator.times_table(3)

    #print(answer3)
    print(df)
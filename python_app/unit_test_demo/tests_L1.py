import unittest
from calculator import Calculator

class TestOperations(unittest.TestCase):
    
    def test_sum(self):
        myCalc = Calculator(2,2)
        answer = myCalc.get_sum()
        self.assertEqual(answer, 4, 'The sum is wrong.')

    def test_diff(self):
        myCalc = Calculator(2,1)
        answer = myCalc.get_diff()
        self.assertEqual(answer, 1, 'The difference is wrong.')

    def test_product(self):
        myCalc = Calculator(2,3)
        answer = myCalc.get_product()
        self.assertEqual(answer, 6, 'The product is wrong.')

    def test_quotient(self):
        myCalc = Calculator(3,1)
        answer = myCalc.get_quotient()
        self.assertEqual(answer, 3, 'The quotient is wrong.')

    def test_sqrt(self):
        myCalc = Calculator(25)
        answer = myCalc.get_sqrt()
        self.assertEqual(answer, 5, 'The square root is wrong.')

if __name__ == '__main__':
    unittest.main()
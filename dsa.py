class car:
    
    def __init__(self, car_model, car_make):
        self.model = car_model
        self.make = car_make

class car_company:

    def __init__(self, company_name, year_established):
        self.name = company_name
        self.est = year_established

my_company = car_company("Infiniti", 1989)

print(my_company.name)

my_car = car("Q50", my_company)

print(my_car.make)
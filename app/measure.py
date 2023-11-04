
import numpy as np

class measure_base:

    def __init__(self, values):
        self.values = np.array(values)
        self.calc()

   
    def calc(self):
        self.sum = np.sum(self.values)
        self.min = np.min(self.values)
        self.max = np.max(self.values)
        self.freq = len(self.values)

    def __add__(self, other):
        new = copy.deepcopy(self)
        new.sum += other.sum
        new.min = np.min([self.min, other.min])
        new.max = np.max([self.max, other.max])
        new.freq += other.freq
        return new

class measure_freq(measure_base):
   

    def __init__(self, values):
        super().__init__(values)
        self.name = "freq"


    def calc(self):
        super().calc()
        self.ret = self.freq
    
    def __add__(self,other):
        new = super().__add__(other)

        new.ret = new.freq

        return new

    def view(self):
        print({"sum":self.sum, "freq": self.freq, "min":self.min, "max":self.max, "mean":self.mean})


class measure_sum(measure_base):
   

    def __init__(self, values):
        super().__init__(values)
        self.name = "sum"


    def calc(self):
        super().calc()
        self.ret = self.sum
    
    def __add__(self,other):
        new = super().__add__(other)

        new.ret = new.sum

        return new

    def view(self):
        print({"sum":self.sum, "freq": self.freq, "min":self.min, "max":self.max, "mean":self.mean})


class measure_mean(measure_base):
   
    def __init__(self, values):
        super().__init__(values)
        self.name = "mean"

    def calc(self):
        super().calc()
        self.mean = np.mean(self.values)
        self.ret = self.mean
    
    def __add__(self,other):
        new = super().__add__(other)

        new.mean = new.sum / new.freq
        new.ret = new.mean

        return new

    def view(self):
        print({"sum":self.sum, "freq": self.freq, "min":self.min, "max":self.max, "mean":self.mean})



class measure_var(measure_mean):
   
    def __init__(self, values):
        super().__init__(values)
        self.name = "var"

    def calc(self):
        super().calc()
        self.diff = sum((self.values - self.mean) ** 2)
        self.var =  self.diff / self.freq
        # self.var = np.var(self.values)
        self.ret = self.var
    
    def __add__(self,other):
        new = super().__add__(other)
        
        new.diff =  (self.mean - new.mean)  ** 2 * self.freq  + self.diff + \
                    (other.mean - new.mean) ** 2 * other.freq + other.diff


        new.var = new.diff / new.freq
        new.ret = new.var

        return new

    def view(self):
        print({"sum":self.sum, "freq": self.freq, "min":self.min, "max":self.max, "mean":self.mean, "var":self.var})


class measure_std(measure_var):
   
    def __init__(self, values):
        super().__init__(values)
        self.name = "std"

    def calc(self):
        super().calc()
        self.std = np.sqrt(self.var)
        self.ret = self.std
    
    def __add__(self,other):
        new = super().__add__(other)

        new.std = np.sqrt(new.var)
        new.ret = new.std

        return new

    def view(self):
        print({"sum":self.sum, "freq": self.freq, "min":self.min, "max":self.max, "mean":self.mean, "var":self.var})

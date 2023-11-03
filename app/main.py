import copy
import time
import statistics
import pandas as pd
import numpy as np
from operator import add
from functools import reduce

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



def agg_core(r, dimensions, measures):
    return r.groupby(dimensions, as_index=False).agg(measures)

def acume_core(r, dimensions):
    return r.groupby(dimensions, as_index=False).agg(lambda x: reduce(add, x))

def trim_array(array):
    return [s for s in array if s.strip() != ""]



class StatTable:

    def __init__(self):
        pass

    def create_using_csv(self, src, dimensions, measures, chunksize = 0):

        time_start = time.time()

        dimensions = [dimension.lower() for dimension in dimensions]
        measures = {key.lower():measures[key] for key in measures.keys()}

        items = dimensions + list(measures.keys())

        if chunksize == 0:
            reader = [pd.read_csv(src, usecols= lambda x: x.lower() in items)]
        else:
            reader = pd.read_csv(src, usecols= lambda x: x.lower() in items, chunksize=chunksize )


        base = pd.concat([agg_core(r.rename(columns=str.lower), dimensions, measures) for r in reader])

        dest = self.__summary(base,dimensions, measures)

        print(dest)

        time_end = time.time()
        tim = time_end- time_start
        print(f'処理時間:{tim}秒')


    def create_using_parquet(self, src, dimensions, measures):

        time_start = time.time()

        items = dimensions + list(measures.keys())

        reader = [pd.read_parquet(src, columns = items)]

        base = pd.concat([agg_core(r, dimensions, measures) for r in reader])

        dest = self.__summary(base, dimensions, measures)

        print(dest)

        time_end = time.time()
        tim = time_end- time_start
        print(f'処理時間:{tim}秒')


    def __summary(self,base, dimensions, measures):

        base.columns = base.columns.map(lambda x: '.'.join(trim_array(x))).str.replace("measure_","")

        dest = acume_core(base, dimensions)

        names = [f'{key}.{m.__name__.replace("measure_","")}' for key in measures.keys() for m in measures[key]]


        dest[names] = dest[names].apply(lambda rows: [item.ret for item in rows])

        return dest



# -------------------------------------------

if __name__ == "__main__":
    # dimensions = ["rou"]
    dimensions = ["Ken","Rou","Sei"]

    measures = {"Age":[measure_freq, measure_sum, measure_var, measure_std, measure_mean]}
    # method = measure_var


    stattable = StatTable()

    src = "data/inp_100000_col4.csv"
    st_csv = stattable.create_using_csv(src, dimensions, measures, chunksize = 500000)
    print(st_csv)

    src = "data/inp_100000_col100.csv"
    st_csv = stattable.create_using_csv(src, dimensions, measures, chunksize = 500000)
    print(st_csv)


    src = "data/inp_100000_col4.parquet"
    st_csv = stattable.create_using_parquet(src, dimensions, measures)
    print(st_csv)


    src = "data/inp_100000_col100.parquet"
    st_csv = stattable.create_using_parquet(src, dimensions, measures)
    print(st_csv)





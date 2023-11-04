
import pandas as pd
from operator import add
from functools import reduce

class StatTable:

    def __init__(self):
        pass

    def create_using_csv(self, src, dimensions, measures, chunksize = 0):


        dimensions = [dimension.lower() for dimension in dimensions]
        measures = {key.lower():measures[key] for key in measures.keys()}

        items = dimensions + list(measures.keys())

        if chunksize == 0:
            reader = [pd.read_csv(src, usecols= lambda x: x.lower() in items)]
        else:
            reader = pd.read_csv(src, usecols= lambda x: x.lower() in items, chunksize=chunksize )


        base = pd.concat([self.__agg_core(r.rename(columns=str.lower), dimensions, measures) for r in reader])

        dest = self.__summary(base,dimensions, measures)

        return(dest)

    def create_using_parquet(self, src, dimensions, measures):


        items = dimensions + list(measures.keys())

        reader = [pd.read_parquet(src, columns = items)]

        base = pd.concat([self.__agg_core(r, dimensions, measures) for r in reader])

        dest = self.__summary(base, dimensions, measures)

        return(dest)

    def __summary(self,base, dimensions, measures):

        base.columns = base.columns.map(lambda x: '.'.join(self.__trim_array(x))).str.replace("measure_","")

        dest = self.__acume_core(base, dimensions)

        names = [f'{key}.{m.__name__.replace("measure_","")}' for key in measures.keys() for m in measures[key]]


        dest[names] = dest[names].apply(lambda rows: [item.ret for item in rows])

        return dest

    def __agg_core(self, r, dimensions, measures):
        return r.groupby(dimensions, as_index=False).agg(measures)

    def __acume_core(self, r, dimensions):
        return r.groupby(dimensions, as_index=False).agg(lambda x: reduce(add, x))

    def __trim_array(self, array):
        return [s for s in array if s.strip() != ""]


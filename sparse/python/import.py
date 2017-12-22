from import_df import load_df
from datetime import datetime
import sys

def main(argv):
    df = load_df(argv[1])
    print(df.df)
'''
    def cb(r):
        birth_date = datetime.strptime(r['birth_date'], "%Y-%m-%d %H:%M:%S")
        curr_date = datetime.now()
    
        age = curr_date.year - birth_date.year - ((curr_date.month, curr_date.day) < (birth_date.month, birth_date.day))
    
        if(age < 100):
        print ("row " + str(cb.i))
            # print (r)
        cb.i += 1
    cb.i = 0

    load_df(argv[1], cb)
'''

if __name__ == '__main__':
    main(sys.argv)
        

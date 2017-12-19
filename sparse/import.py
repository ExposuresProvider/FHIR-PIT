from import_df import load_df
import sys

def main(argv):
    # df = load_df(argv[1])
    # print(df.df)
    def cb(r):
        print ("row " + str(cb.i))
#        print (r)
        cb.i += 1
    cb.i = 0

    load_df(argv[1], cb)

if __name__ == '__main__':
    main(sys.argv)
        

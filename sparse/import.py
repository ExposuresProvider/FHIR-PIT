from import_df import load_df
import sys

def main(argv):
    df = load_df(argv[1])
    print(df);

if __name__ == '__main__':
    main(sys.argv)
        

import pandas as pd

def load_df(filepath, callback=None, filemeta = "endotype_meta.csv", colmeta = [("icd", "icd_meta.csv"), ("mdctn", "mdctn_meta.csv"),
("loinc", "loinc_meta.csv")]):
    colnames_dict = {}
    for col, meta in colmeta:
        colnames_dict[col] = import_array(meta)

    def colname(x):
        for key, val in colnames_dict.iteritems():
            if x[:len(key)] == key:
                return map(lambda y: x + "_" + y, val)
        return x
    colnames = map(colname, import_array(filemeta))

    return import_sparse(colnames, filepath, callback)

class Input:
    def __init__(self, buf, pos):
        self.buf = buf
        self.pos = pos
    def curr(self):
        return self.buf[self.pos]
    def skip(self, s):
        for j in range(len(s)):
            if self.curr() != s[j]:
                raise NameError("error: expected " + s[j] + " found " + self.curr() + " at " + str(self.pos))
            self.next()
    def next(self):
        self.pos += 1
    def eof(self):
        return self.pos == len(self.buf)
    def getPos(self):
        return self.pos
            
def parse_array(line):
    row = []
    inp = Input(line, 0)
    if inp.curr() == "{":
        inp.next()
    while True:
        s = parse_unquoted_string(inp)
        row.append(s)
        if inp.eof() or inp.curr() == "}":
            break;
        if inp.curr() == "\\":
            inp.skip("\\,")
        else:
            inp.skip(",")
    if not inp.eof() and inp.curr() == "}":
        inp.next()
    if not inp.eof() and inp.curr() == "\n":
        inp.next()
    
    if not inp.eof():
        raise NameError("error: expected oef found " + inp.curr() + " at " + str(inp.getPos()))
        
    return row

def parse_row(line, colnames):
    row = {}
    i = 0
    col = 0
    inp = Input(line, 0)
    while True:
        parse_entry(inp, row, colnames[col]);
        if inp.eof():
            break;
        inp.skip(",")
        col += 1
    return row

def parse_entry(inp, row, names):
    if isinstance(names, list):
        if inp.curr() == "\"":
            inp.skip("\"")
            entry = parse_sparse_array(inp)
            indices = entry['indices']
            elements = entry['elements']
            for inx, name in enumerate(names):
                if inx in indices:
                    row[name] = elements[indices.index(inx)]
                else:
                    row[name] = ""
            inp.skip("\"")
    else:
        string = parse_unquoted_string(inp)
        row[names] = string
        

def parse_unquoted_string(inp):
    s = ""
    while not (inp.eof() or inp.curr() == ","):
        s += inp.curr()
        inp.next()
    return s
        
def parse_sparse_array(inp):
    inp.skip("(")
    indices = parse_indices(inp)
    inp.skip(",")
    elements = parse_elements(inp)
    inp.skip(")")
    return {'indices': indices, 'elements' : elements}

def parse_indices(inp):
    indices = []
    if inp.curr() == "\"":
        inp.skip("\"\"{")
        while inp.curr() != "}":
            n = parse_int(inp)
            indices.append(n)
            if inp.curr() == ",":
                inp.next()
        inp.skip("}\"\"")
    else:
        inp.skip("{")
        while inp.curr() != "}":
            n = parse_int(inp)
            indices.append(n)
            if inp.curr() == ",":
                inp.next()
        inp.skip("}")

    return indices
    
def parse_elements(inp):
    elements = []
    if inp.curr() == "\"":
        inp.skip("\"\"{")
        while inp.curr() != "}":
            n = parse_string4(inp)
            elements.append(n)
            if inp.curr() == ",":
                inp.next()
        inp.skip("}\"\"")
    else:
        inp.skip("{")
        while inp.curr() != "}":
            n = parse_string2(inp)
            elements.append(n)
            if inp.curr() == ",":
                inp.next()
        inp.skip("}")
    return elements

def parse_int(inp):
    s = ""
    while not inp.eof() and inp.curr().isdigit():
        s += inp.curr()
        inp.next()
    return int(s)

def parse_string2(inp):
    if inp.curr() == "\"":
        inp.skip("\"\"")
        s = parse_quoted_string(inp)
        inp.skip("\"\"")
    else:
        s = parse_unquoted_string(inp)
    return s

def parse_string4(inp):
    if inp.curr() == "\"":
        inp.skip("\"\"\"\"")
        s = parse_quoted_string(inp)
        inp.skip("\"\"\"\"")
    else:
        s = parse_unquoted_string(inp)
    return s
            
def parse_unquoted_string(inp):
    s = ""
    while not (inp.eof() or inp.curr() in "(){}\\,\""):
        s += inp.curr()
        inp.next()
    return s
    
def parse_quoted_string(inp):
    s = ""
    while not (inp.eof() or inp.curr() == "\""):
        s += inp.curr()
        inp.next()
    return s

def wrap(x):
    if isinstance(x, list):
        return x
    else:
        return [x]
        
def import_sparse(colnames, filepath, callback=None):
    if callback == None:
        def cb(r):
            dfr = pd.DataFrame([r]) #.to_sparse()
            cb.df = cb.df.append(dfr)
        cb.df = pd.DataFrame(columns=sum(map(wrap, colnames),[])) #.to_sparse()
        callback2 = cb
    else:
        callback2 = callback
        
    with open(filepath) as f:
        line = f.readline()
        while line:
            callback2(parse_row(line, colnames))
            line = f.readline()
    return callback2
         
def import_array(filepath):
    with open(filepath) as f:
        line = f.readline()
        return parse_array(line)



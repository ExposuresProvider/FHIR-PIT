package import_df;

use warnings;
use strict;

my $debug = 0;
my $entry_sep = "!";
my $col_sep = ",";

my $rowno = 0;
my $filemeta = "";

use Input;

use constant { true => 1, false => 0 };
if(false) { print 1 }


=pod

=head1 NAME

import_df - imports a dataframe from wide format, enables each row to pass through a callback

=head1 About

This Perl module supports HUSH+ data extracts for machine learning and other computational algorithms. There are also Python and C++ interfaces.

=head1 load_df

Each row of the wide-format HUSH+ data can pass through a use-specified callback, e.g.:

use strict;
use warnings;

from datatrans.sparse.perl.import_df import load_df;
sub cb {
    my $r = shift;
    $birth_date = datetime.strptime(r['birth_date'], "%Y-%m-%d %H:%M:%S");
    $curr_date = datetime.now();
    $age = curr_date.year - birth_date.year - ((curr_date.month, curr_date.day) < (birth_date.month, birth_date.day));
    print(".");
    if($age <= 50) {
        print($r);
}
load_df(filepath => "endotype.csv", callback => \$cb);

=filepath

The fully-qualified path to the wide-format table

=filemeta

OPTIONAL - The fully-qualified path to the meta-data describing the columns for the wide-format table

=colmeta

OPTIONAL - The fully-qualified path to the meta-data describing those columns in the wide-format table that have multiple values
(e.g., ICD, MDCTN, LOINC, VITAl)

=callback

A reference to the function that will be called on each row of the wide-format table. Column name format and referencing described elsewhere

=cut

my %colnames_dict;
sub load_df {
    my $filepath  = shift;
    my $callback =  shift;
    $filemeta = shift;

    # defaults:
    $callback //= undef;
    $filemeta //= "endotype_meta.csv";
#    my %default = (
    my %colmeta = (
        icd => "icd_meta.csv",
        mdctn => "mdctn_rxnorm_meta.csv",
        gene => "mdctn_gene_meta.csv",
        loinc => "loinc_meta.csv",
        vital => "vital_meta.csv",
        );
#    my %colmeta = (%default, @_);

    # for each column meta file
    foreach my $col (keys %colmeta) {
        print("loading colmeta($col) " . $colmeta{$col} . "\n");
        $colnames_dict{$col} = import_array($colmeta{$col}); # xxx just read in file & store the ,-del'd string
    }

    # pass in a token from the primary file meta-data (e.g., endotype_meta.csv)
    sub colname {
        my $x = shift;
        # for array-reference of tokens found in a meta file...
        foreach my $k (keys %colnames_dict) {
            # does the first part of the primary file token match?
            if ($x =~ m/^$k/) {
                # e.g., Given x -> "loinc_valtype", key -> "loinc", first token in "loinc"_meta.csv:
                #  "loinc_valtype" + "LOINC:13834-7_1" -> "loinc_valtype_LOINC:13834-7_1"
                #   ...etc.
                my @ret = map {$x . "_" . $_} ( @{$colnames_dict{$k}} );
                return \@ret;
            }
        }
        return $x;
    }

    print(">loading filemeta ".$filemeta."\n");
    my @colnames = map(colname($_), @{import_headers($filemeta)});
    if($debug) {
        for(my $i = 0; $i < scalar @colnames; $i++) {
            print "[ $i ]: ";
            if ( ref($colnames[$i]) eq "ARRAY" ) {
                print "array> ".join(",", @{$colnames[$i]} )."\n";
            } else {
                print "str>$colnames[$i]\n";
            }
        }
    }

    print("+loading rows\n");
    return import_sparse(\@colnames, $filepath, $callback);
}

=head2 skip_array_sep
If current char is a separator(","), skip over it and return TRUE

=over

=side effect
Skip past the ',' (array separator) iff the current position in the Input object (line) points at one.

=return

TRUE if the Input object (line) is currently at a ',' (array separator), after skipping it
FALSE otherwise

=Input

Reference to an instance of the Input class

=cut
sub skip_array_sep{
    my $inp=shift;
    if ($inp->curr() eq ",") {
        $inp->skip(",");
        return true;
    } else{
        return false;
    }
}

=head 2 skip_entry_sep

If current char is an "entry separator" (record delimiter), skip over it and return TRUE

=over

=side-effect
Skip past the "entry separator" iff the current position in the Input object (line) points at one.

=return

TRUE if the Input object (line) is currently at an "entry separator", then skip it
FALSE otherwise

=Input

Reference to an instance of the Input class

=cut
sub skip_entry_sep {
    my $inp = shift;
    if($inp->curr() eq $entry_sep) {
        $inp->skip($entry_sep);
        return true;
    } else {
        return false;
    }
}
=head 2 parse_array

Parse an '/n'-terminated line.

BNF grammar for endotype.csv file (not considering meta-data):

<line>               ::= <entry>"\n" | <entry><entry_sep><line>
<entry_sep>          ::= "!"
<entry>              ::= <element> | (<index-element-list>) | ""
<index-element-list> ::= <index-container>,<element-container>
<element-container>  ::= '{'<element-list>'}' | '"'{<element-list>}'"' |  '"'{'"'<element-list>'"'}'"'
<index-container>    ::= '{'<index-list>} | '"'{<index-list>}'"' |  '"'{'"'<index-list>'"'}'"'
<element-list>       ::= <string> | <string>","<element-list>
<index-list>         ::= <integer> | <integer>","<index-list>

<integer>            ::=<digit>|<digit><integer>
<string>             ::=""|<character><string>
<character>          ::= <letter> | <digit> | <symbol>
<letter>             ::= "A" | "B" | "C" | "D" | "E" | "F" | "G" | "H" | "I" | "J" | "K" | "L" | "M" | "N" | "O" | "P" | "Q" | "R" | "S" | "T" | "U" | "V" | "W" | "X" | "Y" | "Z" | "a" | "b" | "c" | "d" | "e" | "f" | "g" | "h" | "i" | "j" | "k" | "l" | "m" | "n" | "o" | "p" | "q" | "r" | "s" | "t" | "u" | "v" | "w" | "x" | "y" | "z"
<digit>              ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
<symbol>             ::=  "|" | " " | "-" | "!" | "#" | "$" | "%" | "&" | "(" | ")" | "*" | "+" | "," | "-" | "." | "/" | ":" | ";" | ">" | "=" | "<" | "?" | "@" | "[" | "\" | "]" | "^" | "_" | "`" | "{" | "}" | "~" | "'" | '"'

=over

=side-effect
Input points past the (possibly {}-packaged) list

=return

Reference to an array of quoted-strings(e.g., " or \), parsed-out of the "<line>"

=buf

A single '\n'-terminated line to parse.
=cut
sub parse_array {
    my $line = shift;

    my $row = [];
    my $inp = Input->new(buf => $line, pos => 0);
    if ($inp->curr() eq "{") {
        $inp->next();
    }
    my $array_sep = true;
    while( $array_sep) {
        my $s = parse_string($inp);
        push @{$row}, $s;
        if($inp->eof() || $inp->curr() eq "}") {
            last;
        }
        $array_sep = skip_array_sep($inp);
    }
    if(!$inp->eof() && $inp->curr() eq "}") {
        $inp->next();
    }
    if(!$inp->eof() && $inp->curr() eq "\n") {
        $inp->next();
    }

    if(!$inp->eof()){
        die("+error: expected oef found " . $inp->curr() . " at " . $inp->getPos());
    }

    return $row;
}

=head 2 parse_headers
Called by import_headers
=cut
sub parse_headers {
    my $line = shift;

    my $row = [];
    my $inp = Input->new(buf => $line, pos => 0);
    my $array_sep = true;
    while($array_sep) {
        my $s = parse_string($inp);
        push @{$row} ,  $s;
        if($inp->eof()) {
            last;
        }
        $array_sep = skip_entry_sep($inp);
    }
    if(!$inp->eof() && $inp->curr() eq "\n") {
        $inp->next();
    }

    if(!$inp->eof()) {
        die("++error: expected oef found (" . $inp->curr() . ") at " . $inp->getPos());
    }

    return $row;
}

=head 2 parse_row

Results are given to the user-supplied callback for each row parsed in
hte wide-format file.

=cut
sub parse_row {
    my $line = shift;
    my $colnames = shift;# reference to an array of every column name in wide format

    my $row = {};
    my $i = 0;
    my $col = 0;
    my $inp = Input->new(buf => $line, pos => 0);
    my $array_sep = true;
    while($array_sep) {
        parse_entry($inp, $row, ${$colnames}[$col]);
        if($inp->eof) {
            last;
        }
        $array_sep = skip_entry_sep($inp);
        $col += 1;
    }
# xxx add in error checking of ill-formed meta files, especially the main file
    return $row;
}

=head 2 parse_entry
    Fills in row array_ref with entries from Input instance to be used by callback function
=cut
sub parse_entry {
    my ($inp, $row, $names) = @_;
    if ( ref($names) eq "ARRAY" ) {
        if ($inp->curr() eq "\""){
            $inp->skip("\"");
            my %entry = parse_sparse_array($inp);
            my $indices = $entry{'indices'};
            my $elements = $entry{'elements'};
            for(my $inx=0; $inx < scalar @$indices; $inx++) {
                my $index=${$indices}[$inx];
                ${$row}{ ${$names}[$index-1] } = ${$elements}[$inx];
            }
            $inp->skip("\"");
        } elsif ($inp->curr() eq "(") {
            my %entry = parse_sparse_array($inp);
            my $indices = $entry{'indices'};
            my $elements = $entry{'elements'};
            for(my $inx=0; $inx < scalar @$indices; $inx++) {
                my $index=${$indices}[$inx];
                if(defined(${$names}[$index-1])) {
                    ${$row}{ ${$names}[$index-1] } = ${$elements}[$inx];
                } else {
                    die("ERROR:\"$filemeta\"[row=$rowno,entry=$index], "
                        ."list-item=$inx is undefined, element=[".${$elements}[$inx]."], check meta file format.\n") ;
                }
            }
        } else {
            my %entry = ();
        }

    } else {
        my $s = parse_unquoted_string($inp);
        ${$row}{$names} = $s;
    }
}

# xxx why is this defined 2x?
#=head 2 parse_unquoted_string
#=cut
#sub parse_unquoted_string {
#    my $inp = shift;
#    my $s = "";
#    while (!($inp->eof || $inp->curr() eq ",")) {
#       $s .= $inp->curr();
#        $inp->next();
#    }
#    return $s;
#}

=head 2 parse_sparse_array
   Takes Input instance pointing to the beginning of an array (e.g., "(..)")
=over

=return

   A hash with only two elements: 'indices'=>array_ref, 'elements'=>array_ref

=cut
sub parse_sparse_array {
    my $inp = shift;
    $inp->skip("(");
    my $indices = parse_indices($inp);
    skip_array_sep($inp);
    my $elements = parse_elements($inp);
    $inp->skip(")");
    return ('indices' => $indices, 'elements' => $elements);
}

=head 2 parse_indices

Called by parse_sparse_array

=cut
sub parse_indices {
    my $inp = shift;
    my $indices = [];
    if($inp->curr eq "\"") {
        $inp->skip("\"{");
        while($inp->curr() ne "}") {
            my $n = parse_int($inp);
            push @{$indices}, $n;
            skip_array_sep($inp);
        }
        $inp->skip("}\"");
    } else {
        $inp->skip("{");
        while($inp->curr() ne "}"){
            my $n = parse_int($inp);
            push @{$indices}, $n;
            skip_array_sep($inp);
        }
        $inp->skip("}");
    }
    return $indices;
}

=head 2 parse_elements

Called by parse_sparse_array
Returns a reference to an array of strings.
The original list is encapsulated in "{<something complicated>}" or {}

=cut
sub parse_elements {
    my $inp = shift;
    my $elements = [];
    if($inp->curr() eq "\""){
        $inp->skip("\"{");
        while($inp->curr() ne "}"){
            my $n = parse_string2($inp);
            push @{$elements}, $n;
            skip_array_sep($inp);
        }
        $inp->skip("}\"");
    } else {
        $inp->skip("{");
        while($inp->curr() ne "}") {
            my $n = parse_string($inp);
            push @{$elements}, $n;
            skip_array_sep($inp);
        }
        $inp->skip("}");
    }
    return $elements;
}
=head 2 parse_int
    Called by parse_indices.
    Reads all following digits from current Input instance and returns an integer.
=cut
sub parse_int {
    my $inp = shift;
    my $s = "";
    while(!$inp->eof() && ($inp->curr() =~ /[0-9]/) ) {
        $s .= $inp->curr();
        $inp->next();
    }
    return $s;
}

=head 2 parse_string
=cut
sub parse_string {
    my $inp = shift;
    my $s;
    if ($inp->curr() eq "\"") {
        $inp->skip("\"");
        $s = parse_quoted_string($inp);
        $inp->skip("\"");
    } else {
        $s = parse_unquoted_string($inp);
    }
    return $s;
}

=head 2 parse_string2
    skips any \'s, but the file doesn't seem to have \'s? xxx
=cut
sub parse_string2 {
    my $inp = shift ;
    my $s;
    if($inp->curr() eq "\""){
        $inp->skip("\"\"");
        if($inp->curr() eq "\\"){
            $inp->skip("\\\\\\\\\"\"");
        }
        $s = parse_quoted_string($inp);
        if($inp->curr() eq "\\") {
            $inp->skip("\\\\\\\\\"\"");
        }
        $inp->skip("\"\"");
    } else {
        $s = parse_unquoted_string($inp);
    }
    return $s;
}

=head 2 parse_string4
=cut
sub parse_string4 {
    my $inp = shift;
    my $s;
    if($inp->curr() eq "\"") {
        $inp->skip("\"\"\"\"");
        $s = parse_quoted_string($inp);
        $inp->skip("\"\"\"\"");
    } else {
        $s = parse_unquoted_string($inp);
    }
    return $s;
}

# xxx why is this defined twice in python version?
=head 2 parse_unquoted_string
=cut
sub parse_unquoted_string {
    my $inp = shift;
    my $s = "";
    while( !($inp->eof() || $inp->curr() =~ m/[\n$entry_sep\\,\"\{\}]/ ) ) {
        $s .= $inp->curr();
        $inp->next();
    }
    return $s;
}

=head 2 parse_quoted_string
=cut
sub parse_quoted_string {
    my $inp = shift;
    my $s = "";
    while( !($inp->eof() || $inp->curr() eq "\"" || $inp->curr() eq "\\") ){
        $s .= $inp->curr();
        $inp->next();
    }
    return $s;
}

=head 2 wrap
=cut
sub wrap {
    return shift; #  xxx I think perl takes care of this casting

#    my $x shift;
#    if(isinstance($x, $list)) { # is $x an array?
#        return $x; # yes? return it as itself
#    } else {
#        return [$x]  # no? make it a list
#    }

}

=head 2 import_sparse

=cut
sub import_sparse {
    my ($colnames, $filepath, $callback) = @_;
    # defaults:
    $callback //= undef;
    my $callback2;
#    if($callback == undef){ # xxx ignore this case right now
    if(0) {
        sub cb{
            # xxx what's the equiv of a dataframe?
            my $r = shift;
# xxx won't compile:
#xxx        my $dfr = pd.DataFrame([$r]); #.to_sparse()
#xxx        my $cb_df = cb.df.append($dfr);
        }
        # xxx
#        cb.df = pd.DataFrame(columns=sum(map(wrap, $colnames),[])); #.to_sparse
#       xxx maybe use Statistics::R to do this
        $callback2 = \&cb;
    } else {
        $callback2 = $callback;
    }

    open( FP, "<", $filepath) or die ("Cannot open $filepath");
    while(<FP>) {
        $rowno++;
        $callback2->(parse_row($_, $colnames), $colnames);
    }

    return $callback2;
}

=head 2 import_array
Calls parse_array with the first line of the input file; returns reference to an array of tokens
=cut
sub import_array {
    my $filepath = shift;
    open(FP, "<", $filepath) or die ("can't open $filepath\n");
    my $line = <FP>;
    return parse_array($line);
}

=head 2 import_headers
Import the column-name-meta-data from the adjoining file, e.g.:

patient_num!encounter_num!inout_cd!start_date!end_date!icd_code!icd_start_date!icd_end_date!loinc_valtype!loinc_nval!loinc_tval!loinc_units!loinc_start_date!loinc_end_date!loinc_valueflag!mdctn_valtype!mdctn_nval!mdctn_tval!mdctn_units!mdctn_start_date!mdctn_end_date!mdctn_valueflag!gene!vital_valtype!vital_nval!vital_tval!vital_units!vital_start_date!vital_end_date!vital_valueflag!birth_date!sex_cd!race_cd!lat!long

=over

=side effect
none

=return

reference to an array of column names

=filepath

Path to the file containing the header information. e.g., "endotype_meta.tsv"

=cut
sub import_headers {
    my $filepath = shift;
    open(FP, "<", $filepath) or die ("cannot open file($filepath)");
    my $line = <FP>;
    return parse_headers($line);
}

1;

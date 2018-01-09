#!/usr/bin/perl

use warnings;
use strict;

use import_df;
$|=1;

#from datatrans.sparse.perl.import_df import load_df;
sub cb {
    my $r = shift;
    my $colnames = shift;

    my $row=$r;
    if(1) {
        print "[cb]LINE:\n";
        my $cn="";
        for(my $col=0; $col < scalar @$colnames; $col++) {;
            if(ref(${$colnames}[$col]) eq "ARRAY") {
                foreach $cn ( @{${$colnames}[$col]} ) {
                    if(defined(${$row}{$cn})) {
                        print "[cb]   [$col]/$cn => ${$row}{$cn},\n";
                    }
                }
            } else {
                $cn=${$colnames}[$col];
                if(defined(${$row}{$cn})) {
                    print "[cb] [$col]/$cn => ${$row}{$cn},\n";
                }
            }
        }
        print "\n";
    }
}
import_df::load_df( "endotype.csv", \&cb);

print "Done\n";

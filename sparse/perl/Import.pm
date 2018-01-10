package Input;

use warnings;
use strict;

=pod

=head1 NAME

Input - steps through a line in a wide-format table

=head1 About

Tracks current position in the line

  my $inp = Input->new(buf => $line, pos => 0);
  while(!$inp->eof()) {
    if($inp->curr() == ',') {$inp->skip(",,");}
    $inp->next();
  }

=head1 Constructor

=head2 new

  my $inp = Input->new(buf => $line, pos => 0);

=over

=buf

Contains the string, or line in the wide-format table

=pos

The current offset into the string

=cut

sub new {
    my $class = shift;

    my %options = @_;

    my $self = {
        %options,
    };

    bless($self, $class);
    return($self);
}

=head2 curr
Returns the character pointed to by the current position in the line
=cut
sub curr {
    my $self = shift;
    if(defined($self->{buf})) {
        return substr($self->{buf}, $self->{pos}, 1);
    } else {
        return undef;
    }
}

=head2 skip
Update the current position in the line to point at the character after string 's'. If the current position doesn't match 's', die with an error
=cut
sub skip {
    my $self = shift;
    my $s = shift;
    my $j;
    for( $j = 0; $j < length($s); $j++) {
        if($self->curr() ne substr($s, $j, 1)) {
            die("error: expected " . substr($s,$j,1) . " found " . $self->curr() . " at " . $self->{pos});
        }
        $self->next();
    }
}

=head2 next
    Increment current position in the line
=cut
sub next {
    my $self = shift;
    $self->{pos} += 1;
}

=head2 eof
    Returns true if we've progressed to the end of the line
=cut
sub eof {
    my $self = shift;
    return $self->{pos} == length($self->{buf});
}

=head2 getPos
    return the current position
=cut
sub getPos {
    my $self = shift;
    return $self->{pos};
}

1;
